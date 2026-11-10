package group

import "time"

type smartProfileDefinition struct {
	name                           string
	rank                           []string
	minStability                   int
	target                         float64
	streams                        int
	heartbeat, standby, background time.Duration
	dwell                          time.Duration
	confirmations                  int
	noInterrupt, noThroughput      bool
}

var smartProfiles = []smartProfileDefinition{
	{name: "default"},
	{name: "stability-downtime", rank: []string{"stability", "outages", "latency"}, minStability: -1, target: 4, heartbeat: 10 * time.Second, standby: 30 * time.Second},
	{name: "stability-connections", rank: []string{"stability", "outages", "latency"}, minStability: -1, target: 4, noInterrupt: true, dwell: 30 * time.Minute, confirmations: 3},
	{name: "balance-speed-720p", rank: []string{"speed_floor", "stability", "download", "latency"}, minStability: 2, target: 4},
	{name: "balance-speed-1080p", rank: []string{"speed_floor", "stability", "download", "latency"}, minStability: 2, target: 8},
	{name: "balance-speed-1440p", rank: []string{"speed_floor", "stability", "download", "latency"}, minStability: 2, target: 15},
	{name: "balance-speed-4k", rank: []string{"speed_floor", "stability", "download", "latency"}, minStability: 2, target: 25},
	{name: "speed-max", rank: []string{"download", "stability", "latency"}, minStability: 2, target: 25, streams: 4, dwell: 5 * time.Minute},
	{name: "speed-upload", rank: []string{"upload", "stability", "rtt"}, minStability: 2, target: 25},
	{name: "responsive", rank: []string{"rtt", "stability", "download"}, minStability: 2, target: 4},
	{name: "realtime-calls", rank: []string{"udp", "stability", "jitter", "rtt", "speed_floor"}, minStability: 2, target: 4, heartbeat: 10 * time.Second, standby: 30 * time.Second, noInterrupt: true},
	{name: "realtime-gaming", rank: []string{"udp", "rtt", "jitter", "stability"}, minStability: 2, target: 2, heartbeat: 10 * time.Second, standby: 30 * time.Second, noInterrupt: true},
	{name: "data-saver", rank: []string{"stability", "latency"}, minStability: -1, target: 4, noThroughput: true, heartbeat: 30 * time.Second, standby: 2 * time.Minute, background: 15 * time.Minute},
	{name: "phone", rank: []string{"speed_floor", "stability", "download", "latency"}, minStability: 2, target: 8},
	{name: "desktop", rank: []string{"speed_floor", "stability", "download", "latency"}, minStability: 2, target: 25, streams: 4},
	{name: "server", rank: []string{"speed_floor", "stability", "download", "latency"}, minStability: 2, target: 25, streams: 4, heartbeat: 10 * time.Second, standby: 30 * time.Second},
}
