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

// Package sink is where events go after a poll cycle. The riemann sink
// posts each batch to riemann-go's HTTP ingest from one goroutine: bounded
// queue, drop on overflow, every error swallowed. The dry-run sink prints
// JSON lines instead.
package sink

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// Event is the riemann-go ingest shape (riemann-go SPEC, event model).
type Event struct {
	Host        string            `json:"host"`
	Service     string            `json:"service"`
	State       string            `json:"state"`
	Description string            `json:"description,omitempty"`
	Metric      float64           `json:"metric"`
	Time        float64           `json:"time"` // unix seconds
	TTL         float64           `json:"ttl"`
	Tags        []string          `json:"tags"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

// Sink accepts one batch per cycle. Emit never blocks and never fails.
type Sink interface {
	Emit(batch []Event)
	Close()
}

// Queue depth and flush bound (parameters): 256 batches is over an hour of
// cycles at the default interval; 5 s lets the shutdown marker leave.
const (
	queueDepth   = 256
	flushTimeout = 5 * time.Second
	postTimeout  = 5 * time.Second
)

// Riemann posts batches to <base>/events.
type Riemann struct {
	url  string
	q    chan []Event
	done chan struct{}
}

func NewRiemann(base string) *Riemann {
	r := &Riemann{
		url:  strings.TrimRight(base, "/") + "/events",
		q:    make(chan []Event, queueDepth),
		done: make(chan struct{}),
	}
	go r.drain()
	return r
}

func (r *Riemann) drain() {
	defer close(r.done)
	c := &http.Client{Timeout: postTimeout}
	for batch := range r.q {
		body, err := json.Marshal(batch)
		if err != nil {
			continue
		}
		resp, err := c.Post(r.url, "application/json", bytes.NewReader(body))
		if err != nil {
			continue // swallowed: emission never affects the emitter
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func (r *Riemann) Emit(batch []Event) {
	if len(batch) == 0 {
		return
	}
	select {
	case r.q <- batch:
	default:
	}
}

func (r *Riemann) Close() {
	close(r.q)
	select {
	case <-r.done:
	case <-time.After(flushTimeout):
	}
}

// DryRun prints one JSON object per event per line.
type DryRun struct{ w io.Writer }

func NewDryRun(w io.Writer) *DryRun { return &DryRun{w: w} }

func (d *DryRun) Emit(batch []Event) {
	enc := json.NewEncoder(d.w)
	for _, e := range batch {
		_ = enc.Encode(e)
	}
}

func (d *DryRun) Close() {}
