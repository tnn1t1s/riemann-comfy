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

// Package observe runs one poll cycle and turns what it read into the
// vocabulary of SPEC section 2.
package observe

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-comfy/internal/comfy"
	"github.com/tnn1t1s/riemann-comfy/internal/sink"
)

const tag = "comfy"

// promptTTL keeps a finished prompt's event live in the index for an hour
// (parameter); its time is the prompt's end, which can be minutes old.
const promptTTL = 3600

type Observer struct {
	fennel, app, observerHost string
	history                   int
	ttl                       float64
	seen                      map[string]bool // instance + "/" + prompt id
	lastErr                   string
}

func New(fennel, app, observerHost string, interval time.Duration, history int) *Observer {
	return &Observer{
		fennel:       fennel,
		app:          app,
		observerHost: observerHost,
		history:      history,
		ttl:          3 * interval.Seconds(),
		seen:         map[string]bool{},
	}
}

func (o *Observer) event(service string, metric float64, now time.Time, attrs map[string]string) sink.Event {
	return sink.Event{
		Host:       o.app,
		Service:    service,
		State:      "ok",
		Metric:     metric,
		Time:       float64(now.UnixMicro()) / 1e6,
		TTL:        o.ttl,
		Tags:       []string{tag},
		Attributes: attrs,
	}
}

// logOnce logs a read failure when it differs from the previous one, so a
// down instance does not write a line every cycle.
func (o *Observer) logOnce(msg string) {
	if msg != o.lastErr {
		log.Print(msg)
		o.lastErr = msg
	}
}

// Cycle performs one poll and returns the events to emit.
func (o *Observer) Cycle(ctx context.Context) []sink.Event {
	now := time.Now()
	var out []sink.Event
	targets, err := comfy.Targets(ctx, o.fennel, o.app)
	if err != nil {
		o.logOnce("fennel: " + err.Error())
	}
	answered := 0
	current := map[string]bool{}
	for _, t := range targets {
		stats, err := t.SystemStats(ctx)
		if err != nil {
			o.logOnce(t.Instance + ": " + err.Error())
			continue
		}
		answered++
		a := map[string]string{"instance": t.Instance}
		out = append(out,
			o.event("comfy.ram.free", stats.System.RAMFree, now, a),
			o.event("comfy.ram.total", stats.System.RAMTotal, now, a))
		if len(stats.Devices) > 0 {
			out = append(out,
				o.event("comfy.vram.free", stats.Devices[0].VRAMFree, now, a),
				o.event("comfy.vram.total", stats.Devices[0].VRAMTotal, now, a))
		}
		if running, pending, err := t.Queue(ctx); err == nil {
			out = append(out,
				o.event("comfy.queue.running", float64(running), now, a),
				o.event("comfy.queue.pending", float64(pending), now, a))
		} else {
			o.logOnce(t.Instance + ": " + err.Error())
		}
		prompts, err := t.History(ctx, o.history)
		if err != nil {
			o.logOnce(t.Instance + ": " + err.Error())
			continue
		}
		for _, p := range prompts {
			key := t.Instance + "/" + p.ID
			current[key] = true
			if o.seen[key] {
				continue
			}
			o.seen[key] = true
			out = append(out, o.prompt(p, t.Instance))
		}
	}
	// Forget prompts that fell out of the history window, so seen stays
	// bounded by targets x --history.
	for k := range o.seen {
		if !current[k] {
			delete(o.seen, k)
		}
	}
	if answered > 0 {
		out = append(out, o.event("comfy.heartbeat", float64(answered), now, nil))
	}
	return append(out, o.heartbeat(now, "ok", ""))
}

func (o *Observer) prompt(p comfy.Prompt, instance string) sink.Event {
	state := map[string]string{"success": "ok", "error": "error", "interrupted": "warning"}[p.Outcome]
	a := map[string]string{"instance": instance, "outcome": p.Outcome}
	if len(p.Models) > 0 {
		a["models"] = strings.Join(p.Models, ",")
	}
	e := o.event("comfy.prompt.duration", p.End.Sub(p.Start).Seconds(), p.End, a)
	e.State = state
	e.TTL = promptTTL
	return e
}

func (o *Observer) heartbeat(now time.Time, state, desc string) sink.Event {
	return sink.Event{
		Host:        o.observerHost,
		Service:     "comfy.observer.heartbeat",
		State:       state,
		Description: desc,
		Metric:      1,
		Time:        float64(now.UnixMicro()) / 1e6,
		TTL:         o.ttl,
		Tags:        []string{tag},
		Attributes:  map[string]string{"app": o.app},
	}
}

// Shutdown is the clean-exit marker of SPEC section 1.
func (o *Observer) Shutdown() sink.Event {
	return o.heartbeat(time.Now(), "expired", "completed")
}
