# purge-repro

Two standalone CLI tools for reproducing / observing the
"large-stream-purge stalls JS API for the whole account" issue against any
running NATS cluster.

## Build

```
go build ./tools/purge-repro/fill
go build ./tools/purge-repro/probe
```

## Workflow

In one terminal, fill a stream:

```
./fill \
  --url nats://leader-host:4222 \
  --stream BRONZE_TEST \
  --create \
  --replicas 3 \
  --bytes $((5*1024*1024*1024)) \
  --msg-size $((1024*1024)) \
  --workers 8
```

In another terminal, probe several streams (open-loop, no coordinated
omission):

```
./probe \
  --url nats://leader-host:4222 \
  --streams BRONZE_TEST,SOME_OTHER_STREAM \
  --hz 50 \
  --max-wait 30s \
  --interval 1s
```

Then, from a third terminal, run your purge (or stream edit) and watch the
probe output for latency spikes / timeout errors.

```
nats stream purge BRONZE_TEST --force
```

Hit `Ctrl-C` on the probe to get the final per-stream summary. The `errs`
and `timeout` columns are the smoking gun: requests that didn't return at
all (or hit `max-wait`) during the purge.

## Notes

- `--stream` in `fill` only creates the stream when `--create` (or
  `--recreate`) is set; otherwise it expects the stream to exist.
- The fill tool tries to use `DiscoveredServers()` to pin worker
  connections close to the cluster leader. It falls back to the bootstrap
  URL if discovery isn't available.
- The probe tool uses `js.StreamInfo` as the canary because that's the API
  call that the user's `nats stream get` / `nats stream report` is built
  on; if it stalls, the surveyor and SCP views go silent too.
