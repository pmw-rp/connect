// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"time"
)

var (
	// DefaultSCNWindowSize sets the window size used between SCNs in LogMiner.
	DefaultSCNWindowSize = 20000
	// DefaultMinSCNWindowSize is the minimum SCN gap required before starting a new LogMiner
	// session.
	DefaultMinSCNWindowSize = 1000
	// DefaultMaxSCNWindowSize is the maximum SCN range that can be mined in a single cycle.
	// The adaptive window grows toward this ceiling during backlog and shrinks during steady state.
	DefaultMaxSCNWindowSize = 100000
	// DefaultMiningBackoffInterval controls the mining cycle backoff interval.
	DefaultMiningBackoffInterval = 5 * time.Second
	// DefaultMiningInterval controls the interval between mining cycles during normal operation.
	DefaultMiningInterval = 300 * time.Millisecond
	// DefaultMiningStrategy determines LogMiner's default mining strategy.
	DefaultMiningStrategy = "online_catalog"
	// DefaultWindowingStrategy determines how the mining session's SCN window/upper bound is
	// computed each cycle. Defaults to the legacy adaptive SCN-range strategy for backwards
	// compatibility; log_count is a prototype alternative modeled on Debezium 3.6's approach
	// of sizing mining sessions by a minimum number of redo logs rather than an SCN range.
	DefaultWindowingStrategy = WindowingStrategySCNRange
	// DefaultLogCountMin is the minimum number of redo/archive logs mined per cycle when
	// WindowingStrategy is log_count. Only takes effect under that strategy.
	DefaultLogCountMin = 2
	// DefaultMaxTransactionEvents controls the maximu number of events that can be buffered
	// per transaction before they're discarded.
	// Used to prevent large events resulting in memory exhaustion.
	DefaultMaxTransactionEvents = 0
	// DefaultLOBEnabled controls whether LOB column processing is enabled.
	DefaultLOBEnabled = true
	// DefaultTransactionCacheKey is the default prefix used for the transaction buffer cache key.
	// Only relevant when configuring a (potentially shared) cache_resource transaction buffer.
	DefaultTransactionCacheKey = "oracledb_cdc"
)

// MiningStrategy defines how LogMiner accesses dictionary information
type MiningStrategy string

const (
	// OnlineCatalogStrategy uses the online catalog for dictionary lookups (default, recommended)
	OnlineCatalogStrategy MiningStrategy = "online_catalog"
)

// WindowingStrategy determines how the LogMiner mining session's SCN window is computed each cycle.
type WindowingStrategy string

const (
	// WindowingStrategySCNRange is the legacy strategy: an adaptive SCN-range window that grows/shrinks
	// between min_scn_window_size/max_scn_window_size steps based on whether the connector caught up.
	WindowingStrategySCNRange WindowingStrategy = "scn_range"
	// WindowingStrategyLogCount sizes each mining session by a minimum number of redo/archive logs
	// (log_count_min) rather than an SCN range, growing the count only when a long-running transaction
	// spans more logs than the minimum. Modeled on Debezium 3.6's log-count based LogMiner windowing.
	WindowingStrategyLogCount WindowingStrategy = "log_count"
)

// TransactionCacheConfig contains config specific to service.Cache implementations (ie cache_resources)
type TransactionCacheConfig struct {
	CacheName string
	CacheKey  string
	MaxEvents int
}

// Config holds configuration for LogMiner
type Config struct {
	SCNWindowSize          int
	MinSCNWindowSize       int
	MaxSCNWindowSize       int
	WindowingStrategy      WindowingStrategy
	LogCountMin            int
	MiningBackoffInterval  time.Duration
	MiningInterval         time.Duration
	MiningStrategy         MiningStrategy
	MaxTransactionEvents   int
	LOBEnabled             bool
	PDBName                string
	TransactionCacheConfig TransactionCacheConfig
}

// NewDefaultConfig returns a Config with default values
func NewDefaultConfig() *Config {
	return &Config{
		SCNWindowSize:         DefaultSCNWindowSize,
		MinSCNWindowSize:      DefaultMinSCNWindowSize,
		MaxSCNWindowSize:      DefaultMaxSCNWindowSize,
		WindowingStrategy:     DefaultWindowingStrategy,
		LogCountMin:           DefaultLogCountMin,
		MiningBackoffInterval: DefaultMiningBackoffInterval,
		MiningInterval:        DefaultMiningInterval,
		MiningStrategy:        MiningStrategy(DefaultMiningStrategy),
		MaxTransactionEvents:  DefaultMaxTransactionEvents,
		LOBEnabled:            DefaultLOBEnabled,
	}
}
