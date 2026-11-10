package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	GroupCommonOption
	Default                   string `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool   `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	GroupCommonOption
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
}

type SmartOutboundOptions struct {
	GroupCommonOption
	MemberOptions       map[string]SmartMemberOptions `json:"member_options,omitempty"`
	Probe               SmartProbeOptions             `json:"probe,omitempty"`
	UDP                 SmartUDPOptions               `json:"udp,omitempty"`
	Throughput          SmartThroughputOptions        `json:"throughput,omitempty"`
	InterruptOnFailover *bool                         `json:"interrupt_on_failover,omitempty"`
	NotifyAllDead       *bool                         `json:"notify_all_dead,omitempty"`
}

type SmartMemberOptions struct {
	Tier    int    `json:"tier,omitempty"`
	Country string `json:"country,omitempty"`
}

type SmartProbeOptions struct {
	HeartbeatInterval  badoption.Duration `json:"heartbeat_interval,omitempty"`
	StandbyInterval    badoption.Duration `json:"standby_interval,omitempty"`
	BackgroundInterval badoption.Duration `json:"background_interval,omitempty"`
	Timeout            badoption.Duration `json:"timeout,omitempty"`
	DisableDirect      bool               `json:"disable_direct,omitempty"`
}

type SmartUDPOptions struct {
	Enabled    *bool  `json:"enabled,omitempty"`
	QUICPolicy string `json:"quic_policy,omitempty"`
}

type SmartThroughputOptions struct {
	Enabled               *bool    `json:"enabled,omitempty"`
	TargetMbps            float64  `json:"target_mbps,omitempty"`
	DownloadURL           string   `json:"download_url,omitempty"`
	UploadURL             string   `json:"upload_url,omitempty"`
	WIFIDailyBudgetMB     *float64 `json:"wifi_daily_budget_mb,omitempty"`
	CellularDailyBudgetMB *float64 `json:"cellular_daily_budget_mb,omitempty"`
	OtherDailyBudgetMB    *float64 `json:"other_daily_budget_mb,omitempty"`
}

type FallbackOutboundOptions struct {
	Outbounds        []string           `json:"outbounds"`
	BlacklistTimeout badoption.Duration `json:"blacklist_timeout,omitempty"`
}

type GroupCommonOption struct {
	Outbounds       []string                   `json:"outbounds" reference:"outbound"`
	Providers       badoption.Listable[string] `json:"providers,omitempty"`
	Exclude         *badoption.Regexp          `json:"exclude,omitempty"`
	Include         *badoption.Regexp          `json:"include,omitempty"`
	UseAllProviders bool                       `json:"use_all_providers,omitempty"`
}
