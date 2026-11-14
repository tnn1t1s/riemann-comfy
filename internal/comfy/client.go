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

// Package comfy reads the fennel app record and the ComfyUI routes the
// observer needs. It decodes only the fields SPEC section 2 uses.
package comfy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// requestTimeout bounds every HTTP read (parameter: a cycle must finish
// well inside the default 15 s interval).
const requestTimeout = 5 * time.Second

var httpClient = &http.Client{Timeout: requestTimeout}

func getJSON(ctx context.Context, u string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// Target is one running instance of the app.
type Target struct {
	Instance string // <agent host>:<port>
	BaseURL  string
}

// Targets returns the app's instances whose condition is Running.
func Targets(ctx context.Context, fennel, app string) ([]Target, error) {
	var rec struct {
		Instances []struct {
			Condition string `json:"condition"`
			AgentInfo struct {
				Host string `json:"host"`
			} `json:"agent_info"`
			Ports []int `json:"ports"`
		} `json:"instances"`
	}
	u := strings.TrimRight(fennel, "/") + "/v2/apps/" + strings.TrimLeft(app, "/")
	if err := getJSON(ctx, u, &rec); err != nil {
		return nil, err
	}
	var out []Target
	for _, in := range rec.Instances {
		if in.Condition != "Running" || in.AgentInfo.Host == "" || len(in.Ports) == 0 {
			continue
		}
		hp := fmt.Sprintf("%s:%d", in.AgentInfo.Host, in.Ports[0])
		out = append(out, Target{Instance: hp, BaseURL: "http://" + hp})
	}
	return out, nil
}

// SystemStats holds the memory figures of /system_stats.
type SystemStats struct {
	System struct {
		RAMTotal float64 `json:"ram_total"`
		RAMFree  float64 `json:"ram_free"`
	} `json:"system"`
	Devices []struct {
		VRAMTotal float64 `json:"vram_total"`
		VRAMFree  float64 `json:"vram_free"`
	} `json:"devices"`
}

func (t Target) SystemStats(ctx context.Context) (SystemStats, error) {
	var s SystemStats
	err := getJSON(ctx, t.BaseURL+"/system_stats", &s)
	return s, err
}

// Queue returns the number of running and pending prompts.
func (t Target) Queue(ctx context.Context) (running, pending int, err error) {
	var q struct {
		Running []json.RawMessage `json:"queue_running"`
		Pending []json.RawMessage `json:"queue_pending"`
	}
	if err = getJSON(ctx, t.BaseURL+"/queue", &q); err != nil {
		return 0, 0, err
	}
	return len(q.Running), len(q.Pending), nil
}

// Prompt is one finished prompt from /history.
type Prompt struct {
	ID      string
	Start   time.Time
	End     time.Time
	Outcome string   // success, error or interrupted
	Models  []string // unet_name and ckpt_name inputs, sorted and unique
}

// History returns the finished prompts among the newest maxItems entries.
// Prompts without both a start and a terminal message are left out.
func (t Target) History(ctx context.Context, maxItems int) ([]Prompt, error) {
	var h map[string]struct {
		Prompt []json.RawMessage `json:"prompt"`
		Status struct {
			Messages [][]json.RawMessage `json:"messages"`
		} `json:"status"`
	}
	u := fmt.Sprintf("%s/history?max_items=%s", t.BaseURL, url.QueryEscape(fmt.Sprint(maxItems)))
	if err := getJSON(ctx, u, &h); err != nil {
		return nil, err
	}
	var out []Prompt
	for id, item := range h {
		p := Prompt{ID: id}
		for _, m := range item.Status.Messages {
			if len(m) != 2 {
				continue
			}
			var kind string
			var body struct {
				Timestamp int64 `json:"timestamp"`
			}
			if json.Unmarshal(m[0], &kind) != nil || json.Unmarshal(m[1], &body) != nil || body.Timestamp == 0 {
				continue
			}
			ts := time.UnixMilli(body.Timestamp)
			switch kind {
			case "execution_start":
				p.Start = ts
			case "execution_success":
				p.End, p.Outcome = ts, "success"
			case "execution_error":
				p.End, p.Outcome = ts, "error"
			case "execution_interrupted":
				p.End, p.Outcome = ts, "interrupted"
			}
		}
		if p.Start.IsZero() || p.End.IsZero() {
			continue
		}
		if len(item.Prompt) > 2 {
			p.Models = models(item.Prompt[2])
		}
		out = append(out, p)
	}
	return out, nil
}

// models collects unet_name and ckpt_name input values from a prompt graph.
func models(graph json.RawMessage) []string {
	var nodes map[string]struct {
		Inputs map[string]any `json:"inputs"`
	}
	if json.Unmarshal(graph, &nodes) != nil {
		return nil
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		for _, k := range []string{"unet_name", "ckpt_name"} {
			if s, ok := n.Inputs[k].(string); ok && s != "" {
				seen[s] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
