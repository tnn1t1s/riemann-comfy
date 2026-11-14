// Copyright 2026 tnn1t1s
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command riemann-comfy observes the ComfyUI instances of a fennel app and
// emits riemann events describing them.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tnn1t1s/riemann-comfy/internal/observe"
	"github.com/tnn1t1s/riemann-comfy/internal/sink"
)

func main() {
	var (
		fennel   = flag.String("fennel", "", "base URL of the fennel API (required)")
		app      = flag.String("app", "", "fennel app id; the riemann host of ComfyUI events (required)")
		riemann  = flag.String("riemann", "", "base URL of riemann-go HTTP ingest (required)")
		interval = flag.Duration("interval", 15*time.Second, "poll cadence")
		history  = flag.Int("history", 64, "max_items for each /history read")
		once     = flag.Bool("once", false, "one poll cycle, then exit 0")
		dryRun   = flag.Bool("dry-run", false, "print events as JSON lines instead of emitting")
	)
	flag.Parse()

	var missing []string
	for name, v := range map[string]string{"--fennel": *fennel, "--app": *app, "--riemann": *riemann} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "riemann-comfy: missing required flags: %v\n", missing)
		flag.Usage()
		os.Exit(2)
	}
	if *interval <= 0 || *history <= 0 {
		fmt.Fprintln(os.Stderr, "riemann-comfy: --interval and --history must be positive")
		os.Exit(2)
	}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		fmt.Fprintln(os.Stderr, "riemann-comfy: cannot read hostname for the observer heartbeat:", err)
		os.Exit(2)
	}

	var out sink.Sink
	if *dryRun {
		out = sink.NewDryRun(os.Stdout)
	} else {
		out = sink.NewRiemann(*riemann)
	}
	obs := observe.New(*fennel, *app, "riemann-comfy@"+hostname, *interval, *history)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	out.Emit(obs.Cycle(ctx))
	if !*once {
		ticker := time.NewTicker(*interval)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
				out.Emit(obs.Cycle(ctx))
			}
		}
		ticker.Stop()
	}
	out.Emit([]sink.Event{obs.Shutdown()})
	out.Close()
}
