# riemann-comfy

riemann-comfy polls the ComfyUI instances of one fennel app and sends riemann
events about them to riemann-go's HTTP ingest. It reports queue depth, RAM and
VRAM per instance, and one duration event per finished prompt (outcome and model
filenames, no prompt content). It also sends its own heartbeat, so a silent
observer and a silent ComfyUI are told apart. The fleet's riemann rules index
every event and archive it to InfluxDB. [SPEC.md](SPEC.md) is the contract.

```sh
go build -o riemann-comfy ./cmd/riemann-comfy
./riemann-comfy --fennel "$(medios-endpoint fennel)" --app /ops/comfy-runpod \
  --riemann "$(medios-endpoint riemann)" --once --dry-run
```

Drop `--dry-run` to emit and `--once` to keep polling (default every 15 s).
ComfyUI's API does not report GPU utilization, and it does not record who
submitted a prompt.
