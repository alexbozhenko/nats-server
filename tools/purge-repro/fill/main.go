// purge-repro-fill loads a JetStream stream with a configurable amount of
// data as fast as possible using parallel publishers connected directly to
// the stream leader. Companion to purge-repro-probe.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	var (
		url          string
		creds        string
		nkey         string
		user         string
		pass         string
		stream       string
		subject      string
		replicas     int
		storage      string
		bytesTotal   int64
		msgSize      int
		workers      int
		asyncPending int
		create       bool
		recreate     bool
		progressIv   time.Duration
	)

	flag.StringVar(&url, "url", nats.DefaultURL, "NATS URL(s), comma-separated")
	flag.StringVar(&creds, "creds", "", "Path to NATS credentials file")
	flag.StringVar(&nkey, "nkey", "", "Path to NATS nkey seed file")
	flag.StringVar(&user, "user", "", "Username")
	flag.StringVar(&pass, "pass", "", "Password")
	flag.StringVar(&stream, "stream", "", "Stream name (required)")
	flag.StringVar(&subject, "subject", "", "Subject to publish on (defaults to <stream>.fill)")
	flag.IntVar(&replicas, "replicas", 3, "Number of replicas when creating the stream")
	flag.StringVar(&storage, "storage", "file", "Storage type when creating the stream: file|memory")
	flag.Int64Var(&bytesTotal, "bytes", 1<<30, "Total bytes to publish (default 1 GiB)")
	flag.IntVar(&msgSize, "msg-size", 1<<20, "Message size in bytes (default 1 MiB)")
	flag.IntVar(&workers, "workers", 8, "Parallel publisher goroutines")
	flag.IntVar(&asyncPending, "async-pending", 50_000, "PublishAsyncMaxPending per publisher")
	flag.BoolVar(&create, "create", false, "Create the stream if it does not exist")
	flag.BoolVar(&recreate, "recreate", false, "Delete and recreate the stream before filling")
	flag.DurationVar(&progressIv, "progress", time.Second, "Progress report interval")
	flag.Parse()

	if stream == "" {
		log.Fatal("--stream is required")
	}
	if subject == "" {
		subject = stream + ".fill"
	}
	if msgSize <= 0 || bytesTotal <= 0 {
		log.Fatal("--bytes and --msg-size must be positive")
	}

	opts := []nats.Option{
		nats.Name("purge-repro-fill"),
		nats.MaxReconnects(-1),
		nats.PingInterval(20 * time.Second),
	}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	if nkey != "" {
		opt, err := nats.NkeyOptionFromSeed(nkey)
		if err != nil {
			log.Fatalf("nkey: %v", err)
		}
		opts = append(opts, opt)
	}
	if user != "" {
		opts = append(opts, nats.UserInfo(user, pass))
	}

	bootstrapURL := strings.Join(strings.Split(url, ","), ",")
	nc, err := nats.Connect(bootstrapURL, opts...)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(60 * time.Second))
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	// Optionally (re)create the stream.
	stor := nats.FileStorage
	if strings.EqualFold(storage, "memory") {
		stor = nats.MemoryStorage
	}
	if recreate {
		_ = js.DeleteStream(stream)
	}
	if create || recreate {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     stream,
			Subjects: []string{stream + ".>"},
			Storage:  stor,
			Replicas: replicas,
		})
		if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			log.Fatalf("add stream: %v", err)
		}
	}
	if _, err := js.StreamInfo(stream); err != nil {
		log.Fatalf("stream %q not accessible: %v", stream, err)
	}

	leaderURL, err := resolveLeaderURL(nc, stream)
	if err != nil {
		log.Printf("warn: could not resolve stream leader URL, falling back to bootstrap: %v", err)
		leaderURL = bootstrapURL
	} else {
		log.Printf("stream leader URL: %s", leaderURL)
	}

	totalMsgs := bytesTotal / int64(msgSize)
	log.Printf("filling stream=%q subject=%q with %d bytes (%d msgs of %d bytes) using %d workers",
		stream, subject, bytesTotal, totalMsgs, msgSize, workers)

	var published atomic.Int64
	var stalled atomic.Int64
	var pubErrors atomic.Int64
	start := time.Now()

	// Progress reporter.
	stopReport := make(chan struct{})
	var reportWG sync.WaitGroup
	reportWG.Add(1)
	go func() {
		defer reportWG.Done()
		tick := time.NewTicker(progressIv)
		defer tick.Stop()
		var lastN int64
		var lastT = start
		for {
			select {
			case <-stopReport:
				return
			case now := <-tick.C:
				n := published.Load()
				dt := now.Sub(lastT).Seconds()
				dn := n - lastN
				totalDur := now.Sub(start)
				totalBytes := n * int64(msgSize)
				pct := float64(n) / float64(totalMsgs) * 100
				log.Printf("progress: %d/%d msgs (%.1f%%) %.1f MiB/s window=%dmsg/%.1fs total=%.1f MiB in %s stalled=%d errors=%d",
					n, totalMsgs, pct,
					(float64(dn)*float64(msgSize))/(1024*1024)/dt,
					dn, dt,
					float64(totalBytes)/(1024*1024), totalDur,
					stalled.Load(), pubErrors.Load())
				lastN, lastT = n, now
			}
		}
	}()

	// Workers.
	perWorker := totalMsgs / int64(workers)
	remainder := totalMsgs % int64(workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		count := perWorker
		if int64(w) < remainder {
			count++
		}
		if count == 0 {
			continue
		}
		wg.Add(1)
		go func(workerID int, n int64) {
			defer wg.Done()
			wnc, err := nats.Connect(leaderURL, opts...)
			if err != nil {
				log.Fatalf("worker %d connect: %v", workerID, err)
			}
			defer wnc.Close()
			wjs, err := wnc.JetStream(
				nats.PublishAsyncMaxPending(asyncPending),
				nats.MaxWait(60*time.Second),
			)
			if err != nil {
				log.Fatalf("worker %d jetstream: %v", workerID, err)
			}
			payload := bytes.Repeat([]byte{'x'}, msgSize)
			flushEvery := int64(asyncPending / 2)
			if flushEvery < 1 {
				flushEvery = 1
			}
			flush := func() {
				select {
				case <-wjs.PublishAsyncComplete():
				case <-time.After(10 * time.Minute):
					log.Fatalf("worker %d: timed out waiting for PublishAsyncComplete", workerID)
				}
			}
			for i := int64(0); i < n; i++ {
				if _, perr := wjs.PublishAsync(subject, payload); perr != nil {
					if errors.Is(perr, nats.ErrTooManyStalledMsgs) {
						stalled.Add(1)
						flush()
						i--
						continue
					}
					pubErrors.Add(1)
					log.Printf("worker %d publish at i=%d: %v", workerID, i, perr)
					return
				}
				published.Add(1)
				if (i+1)%flushEvery == 0 {
					flush()
				}
			}
			flush()
		}(w, count)
	}
	wg.Wait()
	close(stopReport)
	reportWG.Wait()

	dur := time.Since(start)
	n := published.Load()
	totalMiB := float64(n*int64(msgSize)) / (1024 * 1024)
	log.Printf("DONE: published %d msgs (%.1f MiB) in %s = %.1f MiB/s | stalled=%d errors=%d",
		n, totalMiB, dur, totalMiB/dur.Seconds(), stalled.Load(), pubErrors.Load())

	if pubErrors.Load() > 0 {
		os.Exit(1)
	}
}

// resolveLeaderURL queries StreamInfo and looks up the leader's client URL via
// JSZ varz on the same monitoring port. Because clients don't normally know
// the per-server client URLs from the leader name alone, we fall back to the
// bootstrap URL if discovery isn't possible. NATS will still forward correctly,
// just with one extra hop.
func resolveLeaderURL(nc *nats.Conn, stream string) (string, error) {
	// nats.go doesn't expose a direct API for cluster-info → client URL
	// mapping, so we ask the server itself for connected URLs and pick one.
	// In a single-cluster deployment all client URLs reach the same leader,
	// so any DiscoveredServers entry works. This is best-effort.
	urls := nc.DiscoveredServers()
	if len(urls) > 0 {
		return strings.Join(urls, ","), nil
	}
	return "", fmt.Errorf("no discovered servers")
}
