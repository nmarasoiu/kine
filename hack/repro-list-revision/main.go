// Command repro-list-revision checks two etcd guarantees that Kubernetes
// relies on, using nothing but the etcd v3 API against a running kine:
//
//  1. Snapshot: a range response with header revision R reflects every write
//     whose revision is <= R.
//  2. List+watch: an informer that lists at R and then watches from R+1
//     converges to the true final state once writes stop.
//
// Writers write keys concurrently; each key is owned by a single writer, so
// per-key acknowledged revisions are monotonic. By default every key is written
// once, so an event an informer misses is never healed by a later write.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var prefix = fmt.Sprintf("/repro/%d/", time.Now().UnixNano())

type history struct {
	sync.Mutex
	acks map[string][]int64 // key -> acknowledged revisions, ascending
}

func (h *history) add(key string, rev int64) {
	h.Lock()
	h.acks[key] = append(h.acks[key], rev)
	h.Unlock()
}

// maxAckedAtOrBelow returns the highest acknowledged revision of key that is <= rev.
func (h *history) maxAckedAtOrBelow(key string, rev int64) int64 {
	revs := h.acks[key]
	i := sort.Search(len(revs), func(i int) bool { return revs[i] > rev })
	if i == 0 {
		return 0
	}
	return revs[i-1]
}

type snapshot struct {
	rev  int64
	mods map[string]int64
}

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:2379", "kine endpoint")
	writers := flag.Int("writers", 64, "concurrent writers")
	keysPerWriter := flag.Int("keys", 0, "keys owned by each writer, overwritten round-robin; 0 means every write creates a new key")
	duration := flag.Duration("duration", 20*time.Second, "write phase duration")
	informers := flag.Int("informers", 20, "list+watch informers started during the write phase")
	flag.Parse()

	newClient := func() *clientv3.Client {
		c, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 5 * time.Second})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return c
	}
	cli := newClient()
	defer cli.Close()

	ctx := context.Background()

	h := &history{acks: map[string][]int64{}}
	var writes, writeErrs atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: one client per writer so writes go over separate connections.
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := newClient()
			defer c.Close()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				k := n
				if *keysPerWriter > 0 {
					k = n % *keysPerWriter
				}
				key := fmt.Sprintf("%sw%03d/k%d", prefix, w, k)
				resp, err := c.Put(ctx, key, fmt.Sprintf("%d", n))
				if err != nil {
					writeErrs.Add(1)
					continue
				}
				writes.Add(1)
				h.add(key, resp.Header.Revision)
			}
		}(w)
	}

	// Snapshot checker: repeatedly list and keep every snapshot for checking later,
	// when all acknowledgements are known.
	var snaps []snapshot
	var snapMu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
			if err != nil {
				continue
			}
			s := snapshot{rev: resp.Header.Revision, mods: make(map[string]int64, len(resp.Kvs))}
			for _, kv := range resp.Kvs {
				s.mods[string(kv.Key)] = kv.ModRevision
			}
			snapMu.Lock()
			snaps = append(snaps, s)
			snapMu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Informers: list at R, then watch from R+1, applying events to a local cache,
	// exactly as a Kubernetes reflector does.
	type informer struct {
		listRev int64
		mu      sync.Mutex
		cache   map[string]int64
	}
	infs := make([]*informer, *informers)
	infCtx, infCancel := context.WithCancel(ctx)
	var infWG sync.WaitGroup
	for i := range infs {
		time.Sleep(time.Duration(float64(*duration) / float64(*informers+1) * (0.5 + rand.Float64())))
		inf := &informer{cache: map[string]int64{}}
		infs[i] = inf
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
		if err != nil {
			fmt.Fprintln(os.Stderr, "informer list:", err)
			os.Exit(2)
		}
		inf.listRev = resp.Header.Revision
		for _, kv := range resp.Kvs {
			inf.cache[string(kv.Key)] = kv.ModRevision
		}
		wc := newClient()
		wch := wc.Watch(infCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(inf.listRev+1))
		infWG.Add(1)
		go func() {
			defer infWG.Done()
			defer wc.Close()
			for wr := range wch {
				inf.mu.Lock()
				for _, ev := range wr.Events {
					inf.cache[string(ev.Kv.Key)] = ev.Kv.ModRevision
				}
				inf.mu.Unlock()
			}
		}()
	}

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	// Let watches drain, then take the ground truth.
	time.Sleep(3 * time.Second)
	infCancel()
	infWG.Wait()

	final, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	truth := map[string]int64{}
	for _, kv := range final.Kvs {
		truth[string(kv.Key)] = kv.ModRevision
	}

	fmt.Printf("writes=%d writeErrors=%d snapshots=%d informers=%d finalRev=%d\n",
		writes.Load(), writeErrs.Load(), len(snaps), len(infs), final.Header.Revision)

	// Check 1: snapshot completeness.
	snapViolations := 0
	for _, s := range snaps {
		for key := range h.acks {
			want := h.maxAckedAtOrBelow(key, s.rev)
			if got := s.mods[key]; got < want {
				snapViolations++
				if snapViolations <= 10 {
					fmt.Printf("SNAPSHOT VIOLATION: list at rev %d shows %s at mod_revision %d, but a write at rev %d (<= %d) was acknowledged\n",
						s.rev, key, got, want, s.rev)
				}
			}
		}
	}

	// Check 2: informer convergence.
	stale := 0
	for i, inf := range infs {
		for key, want := range truth {
			if got := inf.cache[key]; got != want {
				stale++
				if stale <= 10 {
					fmt.Printf("INFORMER %d (listed at rev %d) is permanently stale: %s cached at mod_revision %d, truth %d\n",
						i, inf.listRev, key, got, want)
				}
			}
		}
	}

	fmt.Printf("snapshotViolations=%d staleInformerKeys=%d\n", snapViolations, stale)
	if snapViolations > 0 || stale > 0 {
		os.Exit(1)
	}
}
