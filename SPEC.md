# riemann-comfy — specification

riemann-comfy observes the ComfyUI instances of one fennel app through
ComfyUI's HTTP API and emits riemann events describing them. It is an
external observer: ComfyUI and fennel know nothing about it. All knowledge
comes from fennel's `/v2/apps` API and ComfyUI's `/queue`, `/history` and
`/system_stats` routes.

The normative core is the flag set (§1) and the event vocabulary (§2).

## 1. Observer behavior

One binary, `riemann-comfy`, with flags (normative, no renames, no short
forms):

- `--fennel` — required. Base URL of the fennel API.
- `--app` — required. Fennel app id, for example `/ops/comfy-runpod`. Its
  value is the riemann `host` of every ComfyUI-describing event.
- `--riemann` — required. Base URL of riemann-go's HTTP ingest; events go to
  `POST <riemann>/events` as one JSON array per cycle.
- `--interval` — optional, default `15s`. Poll cadence.
- `--history` — optional, default `64`. `max_items` for each `/history` read.
- `--once` — optional. One poll cycle, then exit 0.
- `--dry-run` — optional. Print events as JSON lines to stdout and emit
  nothing. Composable with `--once`.

Each cycle the observer:

1. `GET <fennel>/v2/apps/<app>`. Every instance with `condition` `Running`
   is a target at `http://<agent_info.host>:<ports[0]>`. A fennel failure
   means no targets this cycle.
2. For each target: `GET /system_stats`, `GET /queue`,
   `GET /history?max_items=<history>`. A target whose `/system_stats` does
   not answer 200 contributes no events.
3. Emits §2 for the targets that answered, then its own observer heartbeat.

Emission is fire-and-forget: bounded queue, drop on overflow, every network
error swallowed, a cycle never blocks on riemann, and the process exits 0.

On clean shutdown (SIGINT, SIGTERM, or `--once` completion) the observer
emits `comfy.observer.heartbeat` with state `expired` and description
`completed`.

## 2. Event vocabulary (normative)

Every event carries tag `comfy`. Unless the table says otherwise, `ttl` is
3 × `--interval` and `time` is the cycle time. Per-instance events carry
attribute `instance` = `<agent_info.host>:<port>`.

| service | host | metric | notes |
|---|---|---|---|
| `comfy.heartbeat` | --app | instances that answered | state `ok`; emitted iff at least one answered |
| `comfy.queue.running` | --app | gauge | per instance |
| `comfy.queue.pending` | --app | gauge | per instance |
| `comfy.vram.free` | --app | bytes | first device of `/system_stats`, per instance |
| `comfy.vram.total` | --app | bytes | same |
| `comfy.ram.free` | --app | bytes | per instance |
| `comfy.ram.total` | --app | bytes | per instance |
| `comfy.prompt.duration` | --app | seconds, execution start to end | one event per finished prompt, see below |
| `comfy.observer.heartbeat` | `riemann-comfy@<hostname>` | 1 | distinct host, so observer death and ComfyUI death expire separately |

`comfy.prompt.duration`:

- Start and end are the `timestamp` fields of the prompt's
  `execution_start` and of its terminal message (`execution_success`,
  `execution_error` or `execution_interrupted`) in `/history`. A prompt
  without both is skipped.
- `time` is the end timestamp, not the cycle time; `ttl` is 3600 s.
- `state`: `ok` for success, `error` for error, `warning` for interrupted.
- Attribute `outcome`: `success`, `error` or `interrupted`.
- Attribute `models`: the sorted, comma-joined, de-duplicated values of
  `unet_name` and `ckpt_name` inputs in the prompt graph, or absent when
  there are none.
- Each prompt id is emitted once per observer process. A restarted observer
  emits the prompts still in `/history` again with identical host, service,
  attributes and time, so the InfluxDB point is overwritten, not duplicated.

The observer emits observations only: no prompt ids, client ids, prompt
text, inputs or outputs. Model filenames are names, not content.

## 3. Persistence and alerting

The fleet's riemann-go rule `index-and-archive` indexes every event and
archives every event that has a metric to InfluxDB (measurement = service,
attributes as tags). This repository adds no rules. An alert on
`comfy.heartbeat` expiry is a rule change in deploy-riemann.

## 4. Validation

1. `--once --dry-run` against a live app: every line matches §2.
2. `--once` against the fleet riemann: `GET <riemann>/index` returns
   `comfy.heartbeat` for the app.
3. On SIGTERM the last event is the observer heartbeat with state `expired`.

## Not here

- GPU utilization. ComfyUI's API does not report it.
- Who submitted a prompt. ComfyUI records only a client-chosen `client_id`.
