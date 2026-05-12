// purge-repro-probe runs an open-loop probe of js.StreamInfo against one or
// more streams at a fixed request rate, prints rolling-window latency stats
// every interval, and a final summary on exit. Open loop means each tick
// fires a fresh request goroutine regardless of whether the previous one
// completed, so a server-side stall is visible in p99/max rather than hidden
// by coordinated omission.
package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type result struct {
	stream  string
	sentAt  time.Time
	latency time.Duration
	err     error
}

func main() {
	var (
		url        string
		creds      string
		nkey       string
		user       string
		pass       string
		streamsCSV string
		hz         int
		maxWait    time.Duration
		duration   time.Duration
		interval   time.Duration
	)
	flag.StringVar(&url, "url", nats.DefaultURL, "NATS URL(s), comma-separated")
	flag.StringVar(&creds, "creds", "", "Path to NATS credentials file")
	flag.StringVar(&nkey, "nkey", "", "Path to NATS nkey seed file")
	flag.StringVar(&user, "user", "", "Username")
	flag.StringVar(&pass, "pass", "", "Password")
	flag.StringVar(&streamsCSV, "streams", "", "Comma-separated list of stream names to probe (required)")
	flag.IntVar(&hz, "hz", 50, "Probe rate per stream (requests/sec)")
	flag.DurationVar(&maxWait, "max-wait", 30*time.Second, "Per-request timeout")
	flag.DurationVar(&duration, "duration", 0, "Stop after this long (0 = run until Ctrl-C)")
	flag.DurationVar(&interval, "interval", time.Second, "Rolling-window report interval")
	flag.Parse()

	streams := strings.Split(streamsCSV, ",")
	for i := range streams {
		streams[i] = strings.TrimSpace(streams[i])
	}
	if len(streams) == 0 || streams[0] == "" {
		log.Fatal("--streams is required (comma-separated)")
	}
	if hz <= 0 {
		log.Fatal("--hz must be positive")
	}

	opts := []nats.Option{
		nats.Name("purge-repro-probe"),
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

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(maxWait))
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	log.Printf("probing streams=%v at %d Hz each (max-wait=%s)", streams, hz, maxWait)

	results := make(chan result, hz*len(streams)*60)
	stop := make(chan struct{})
	var probeWG sync.WaitGroup

	for _, s := range streams {
		probeWG.Add(1)
		go probeStream(&probeWG, js, s, hz, maxWait, results, stop)
	}

	// Live reporter + collector.
	all := make([]result, 0, hz*len(streams)*60)
	collectorDone := make(chan struct{})
	go collect(streams, results, interval, &all, collectorDone)

	// Termination: signal or duration.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	var doneTimer <-chan time.Time
	if duration > 0 {
		doneTimer = time.After(duration)
	}
	select {
	case <-sigCh:
		log.Printf("received signal, stopping...")
	case <-doneTimer:
		log.Printf("duration elapsed, stopping...")
	}
	close(stop)
	probeWG.Wait()
	close(results)
	<-collectorDone

	// Final per-stream summary across the whole run.
	summary(streams, all)
}

func probeStream(wg *sync.WaitGroup, js nats.JetStreamContext, stream string, hz int, maxWait time.Duration, out chan<- result, stop <-chan struct{}) {
	defer wg.Done()
	tick := time.NewTicker(time.Second / time.Duration(hz))
	defer tick.Stop()
	var inFlight sync.WaitGroup
	fire := func() {
		inFlight.Add(1)
		go func() {
			defer inFlight.Done()
			t0 := time.Now()
			_, err := js.StreamInfo(stream, nats.MaxWait(maxWait))
			out <- result{stream: stream, sentAt: t0, latency: time.Since(t0), err: err}
		}()
	}
	for {
		select {
		case <-stop:
			inFlight.Wait()
			return
		case <-tick.C:
			fire()
		}
	}
}

type windowStats struct {
	lats []time.Duration
	errs int
}

func collect(streams []string, in <-chan result, every time.Duration, store *[]result, done chan<- struct{}) {
	defer close(done)
	tick := time.NewTicker(every)
	defer tick.Stop()
	window := map[string]*windowStats{}
	cumLats := map[string][]time.Duration{}
	cumErrs := map[string]int{}
	cumTimeouts := map[string]int{}
	for _, s := range streams {
		window[s] = &windowStats{}
	}
	startedAt := time.Now()
	stats := func(lats []time.Duration) (p50, p99, mx time.Duration) {
		n := len(lats)
		if n == 0 {
			return 0, 0, 0
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		return lats[n*50/100], lats[(n*99)/100], lats[n-1]
	}
	emit := func() {
		elapsed := time.Since(startedAt).Round(time.Second)
		log.Printf("[%s] t=%s", time.Now().Format("15:04:05"), elapsed)
		for _, s := range streams {
			w := window[s]
			wP50, wP99, wMax := stats(w.lats)
			cumP50, cumP99, cumMax := stats(cumLats[s])
			log.Printf("  %-20s window: n=%4d errs=%2d p50=%-10s p99=%-10s max=%-10s  | cum: n=%6d errs=%4d timeout=%4d p50=%-10s p99=%-10s max=%-10s",
				s, len(w.lats), w.errs, wP50, wP99, wMax,
				len(cumLats[s]), cumErrs[s], cumTimeouts[s], cumP50, cumP99, cumMax)
		}
		for _, s := range streams {
			window[s] = &windowStats{}
		}
	}
	for {
		select {
		case r, ok := <-in:
			if !ok {
				emit()
				return
			}
			*store = append(*store, r)
			w, ok := window[r.stream]
			if !ok {
				w = &windowStats{}
				window[r.stream] = w
			}
			if r.err != nil {
				w.errs++
				cumErrs[r.stream]++
				if errors.Is(r.err, nats.ErrTimeout) || errors.Is(r.err, nats.ErrNoResponders) {
					cumTimeouts[r.stream]++
				}
				continue
			}
			w.lats = append(w.lats, r.latency)
			cumLats[r.stream] = append(cumLats[r.stream], r.latency)
		case <-tick.C:
			emit()
		}
	}
}

func summary(streams []string, all []result) {
	log.Printf("=== final summary ===")
	per := map[string][]time.Duration{}
	errs := map[string]int{}
	timeouts := map[string]int{}
	for _, r := range all {
		if r.err != nil {
			errs[r.stream]++
			if errors.Is(r.err, nats.ErrTimeout) || errors.Is(r.err, nats.ErrNoResponders) {
				timeouts[r.stream]++
			}
			continue
		}
		per[r.stream] = append(per[r.stream], r.latency)
	}
	log.Printf("%-20s %8s %6s %8s %12s %12s %12s",
		"stream", "n", "errs", "timeout", "p50", "p99", "max")
	for _, s := range streams {
		ls := per[s]
		sort.Slice(ls, func(i, j int) bool { return ls[i] < ls[j] })
		n := len(ls)
		var p50, p99, mx time.Duration
		if n > 0 {
			p50 = ls[n*50/100]
			p99 = ls[(n*99)/100]
			mx = ls[n-1]
		}
		log.Printf("%-20s %8d %6d %8d %12s %12s %12s",
			s, n, errs[s], timeouts[s], p50, p99, mx)
	}
}
