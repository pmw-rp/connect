// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

func TestProcessRedoEventWithInMemoryCache(t *testing.T) {
	t.Run("single transaction commit", func(t *testing.T) {
		cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})

		require.NoError(t, err)
		require.Len(t, pub.messages, 1)
		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN)
	})

	// When transaction A commits while transaction B is still open, the checkpoint
	// must not advance past B's start SCN - 1. If it did, a restart would begin
	// mining at A's commit SCN and miss B's already-seen DML events — silently
	// losing B's changes when its COMMIT is later encountered.
	t.Run("concurrent transactions commit", func(t *testing.T) {
		cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910)
			txACommit = uint64(1000)
			txBCommit = uint64(1050)
		)

		// Seed both transactions. B remains open when A commits.
		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txB", txBStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		// Commit tranaction A, transaction B still open.
		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		msg := "while B is open, CheckpointSCN must be held back to B.startSCN-1 to avoid skipping transaction B on restart"
		assert.Equal(t, replication.SCN(txBStart-1), pub.messages[0].CheckpointSCN, msg)

		// Commit B — no open transactions remain.
		err = lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txBCommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txB",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 2, "B's commit must publish its events")

		msg = "with no remaining open transactions, CheckpointSCN must equal B's commit SCN"
		assert.Equal(t, replication.SCN(txBCommit), pub.messages[1].CheckpointSCN, msg)
	})

	// A transaction that receives OpStart but no DML events (e.g. a read-only
	// or DDL transaction on an unsubscribed table) must not hold back the
	// checkpoint watermark — it has nothing to replay on restart.
	t.Run("open transaction with no events does not hold back checkpoint", func(t *testing.T) {
		cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910) // starts but never gets DML events
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		// txB is started but never receives any DML events
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		msg := "txB has no DML events so it must not hold back the checkpoint"
		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN, msg)
	})
}

func TestProcessRedoEventWithConnectCacheResource(t *testing.T) {
	newCacheResource := func(t *testing.T) *ConnectCacheResource {
		t.Helper()
		res := service.MockResources(service.MockResourcesOptAddCache("txn_cache"))
		cfg := TransactionCacheConfig{CacheName: "txn_cache", CacheKey: "oracledb_cdc", MaxEvents: 0}
		return NewConnectCacheResource(res, cfg, res.Metrics(), service.NewLoggerFromSlog(slog.Default()))
	}

	t.Run("single transaction commit", func(t *testing.T) {
		cache := newCacheResource(t)
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})

		require.NoError(t, err)
		require.Len(t, pub.messages, 1)
		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN)
	})

	// When transaction A commits while transaction B is still open, the checkpoint
	// must not advance past B's start SCN - 1, otherwise a restart would skip
	// B's already-seen DML events.
	t.Run("concurrent transactions checkpoint held back to lowest open SCN", func(t *testing.T) {
		cache := newCacheResource(t)
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910)
			txACommit = uint64(1000)
			txBCommit = uint64(1050)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txB", txBStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		msg := "while B is open, CheckpointSCN must be held back to B.startSCN-1 to avoid skipping transaction B on restart"
		assert.Equal(t, replication.SCN(txBStart-1), pub.messages[0].CheckpointSCN, msg)

		err = lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txBCommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txB",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 2, "B's commit must publish its events")

		msg = "with no remaining open transactions, CheckpointSCN must equal B's commit SCN"
		assert.Equal(t, replication.SCN(txBCommit), pub.messages[1].CheckpointSCN, msg)
	})

	// A transaction that receives OpStart but no DML events (e.g. a read-only
	// or DDL transaction on an unsubscribed table) must not hold back the
	// checkpoint watermark — it has nothing to replay on restart.
	t.Run("open transaction with no events does not hold back checkpoint", func(t *testing.T) {
		cache := newCacheResource(t)
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910) // starts but never gets DML events
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		// txB is started but never receives any DML events
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN,
			"txB has no DML events so it must not hold back the checkpoint")
	})
}

func TestSessionManagerFilesChanged(t *testing.T) {
	tests := []struct {
		name        string
		loaded      []*LogFile
		incoming    []*LogFile
		wantChanged bool
	}{
		{
			name:        "no files loaded yet",
			loaded:      nil,
			incoming:    []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}},
			wantChanged: true,
		},
		{
			name:        "same files in same order reports unchanged",
			loaded:      []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}, {FileName: "redo02.log", Thread: 1, Sequence: 2}},
			incoming:    []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}, {FileName: "redo02.log", Thread: 1, Sequence: 2}},
			wantChanged: false,
		},
		{
			name:        "more files incoming than loaded",
			loaded:      []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}},
			incoming:    []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}, {FileName: "redo02.log", Thread: 1, Sequence: 2}},
			wantChanged: true,
		},
		{
			name:        "fewer files incoming than loaded",
			loaded:      []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}, {FileName: "redo02.log", Thread: 1, Sequence: 2}},
			incoming:    []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}},
			wantChanged: true,
		},
		{
			name:        "same count but different sequence",
			loaded:      []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}},
			incoming:    []*LogFile{{FileName: "redo02.log", Thread: 1, Sequence: 2}},
			wantChanged: true,
		},
		{
			name:        "same file name reused for a rotated sequence reports changed",
			loaded:      []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}},
			incoming:    []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 5}},
			wantChanged: true,
		},
		{
			name:        "same thread+sequence with incidentally different file name reports unchanged",
			loaded:      []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}},
			incoming:    []*LogFile{{FileName: "redo01_alt_member.log", Thread: 1, Sequence: 1}},
			wantChanged: false,
		},
		{
			name:        "same sequence on a different thread reports changed",
			loaded:      []*LogFile{{FileName: "redo01.log", Thread: 1, Sequence: 1}},
			incoming:    []*LogFile{{FileName: "redo01.log", Thread: 2, Sequence: 1}},
			wantChanged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SessionManager{loadedFiles: tt.loaded}
			assert.Equal(t, tt.wantChanged, sm.logFilesChanged(tt.incoming))
		})
	}
}

func TestCapLogsByCount(t *testing.T) {
	// Each log below is sized as one "unit" of maxLogBytes (1000) unless noted otherwise, so the
	// count parameter maps directly to log.mining.log.count.min semantics from Debezium 3.6.
	const unit = int64(1000)

	archived := func(seq int64, bytes int64) *LogFile {
		return &LogFile{Sequence: seq, Type: "ARCHIVED", IsCurrent: false, Bytes: bytes, NextSCN: uint64(seq * 100)}
	}
	online := func(seq int64, bytes int64) *LogFile {
		return &LogFile{Sequence: seq, Type: "ONLINE", IsCurrent: true, Bytes: bytes, NextSCN: uint64(seq * 100)}
	}
	// inactiveOnline models an online redo log group that has already switched out (Oracle
	// V$LOG.STATUS is INACTIVE/ACTIVE, not CURRENT) but hasn't been archived yet, so it still
	// surfaces from the "ONLINE" branch of GetLogsFromSCN's query. Unlike online(), it has a
	// real, finite NextSCN (the switch already fixed its ending point), just like an archived
	// log. GetLogsFromSCN must report IsCurrent=false for logs like this one — Type alone
	// ("ONLINE" vs "ARCHIVED") does not tell you whether Oracle is still actively writing to a
	// log, only whether it came from V$LOG vs V$ARCHIVED_LOG.
	inactiveOnline := func(seq int64, bytes int64) *LogFile {
		return &LogFile{Sequence: seq, Type: "ONLINE", IsCurrent: false, Bytes: bytes, NextSCN: uint64(seq * 100)}
	}

	tests := []struct {
		name          string
		logs          []*LogFile
		count         int
		maxLogBytes   int64
		wantSequences []int64
		wantAllOnline bool
	}{
		{
			name:          "count of zero disables the cap, returns every log",
			logs:          []*LogFile{archived(1, unit), archived(2, unit), online(3, unit)},
			count:         0,
			maxLogBytes:   unit,
			wantSequences: []int64{1, 2, 3},
			wantAllOnline: true,
		},
		{
			name:          "only the current online log available, no backlog",
			logs:          []*LogFile{online(1, unit)},
			count:         2,
			maxLogBytes:   unit,
			wantSequences: []int64{1},
			wantAllOnline: true,
		},
		{
			name:          "backlog larger than cap truncates before reaching the online log",
			logs:          []*LogFile{archived(1, unit), archived(2, unit), archived(3, unit), archived(4, unit), online(5, unit)},
			count:         2,
			maxLogBytes:   unit,
			wantSequences: []int64{1, 2},
			wantAllOnline: false,
		},
		{
			name:          "backlog smaller than cap reaches the online log, no truncation",
			logs:          []*LogFile{archived(1, unit), online(2, unit)},
			count:         2,
			maxLogBytes:   unit,
			wantSequences: []int64{1, 2},
			wantAllOnline: true,
		},
		{
			name:          "larger log files reach the threshold sooner",
			logs:          []*LogFile{archived(1, unit*3), archived(2, unit), archived(3, unit), online(4, unit)},
			count:         2,
			maxLogBytes:   unit,
			wantSequences: []int64{1},
			wantAllOnline: false,
		},
		{
			name:          "no logs available",
			logs:          nil,
			count:         2,
			maxLogBytes:   unit,
			wantSequences: nil,
			wantAllOnline: false,
		},
		{
			// A switched-out-but-not-yet-archived log must NOT be treated as "no cap needed"
			// just because its Type is ONLINE: it has a real, finite NextSCN like an archived
			// log, and mining only loaded this file, not whatever comes after it.
			name:          "capped set ending on a not-yet-archived online log is not allOnline",
			logs:          []*LogFile{archived(1, unit), inactiveOnline(2, unit), online(3, unit)},
			count:         2,
			maxLogBytes:   unit,
			wantSequences: []int64{1, 2},
			wantAllOnline: false,
		},
		{
			// Same log set, but with a large enough count that capping runs past the
			// not-yet-archived log all the way to the truly current one: allOnline correctly
			// flips to true once the loaded set actually reaches the live log.
			name:          "capped set reaching the truly current log past a not-yet-archived one is allOnline",
			logs:          []*LogFile{archived(1, unit), inactiveOnline(2, unit), online(3, unit)},
			count:         3,
			maxLogBytes:   unit,
			wantSequences: []int64{1, 2, 3},
			wantAllOnline: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capped, allOnline := capLogsByCount(tt.logs, tt.count, tt.maxLogBytes)
			var gotSequences []int64
			for _, lf := range capped {
				gotSequences = append(gotSequences, lf.Sequence)
			}
			assert.Equal(t, tt.wantSequences, gotSequences)
			assert.Equal(t, tt.wantAllOnline, allOnline)
		})
	}
}

func TestEffectiveUpperBound(t *testing.T) {
	tests := []struct {
		name         string
		capped       []*LogFile
		allOnline    bool
		dbCurrentSCN uint64
		want         uint64
	}{
		{
			name:         "allOnline mines uncapped up to the live edge",
			capped:       []*LogFile{{Sequence: 1, Type: "ONLINE", IsCurrent: true, NextSCN: 999999999}},
			allOnline:    true,
			dbCurrentSCN: 5000,
			want:         5000,
		},
		{
			// This is the exact scenario that caused silent data loss: a capped set ending on an
			// archived log whose NextSCN (4936937, say) is the FirstSCN of the next, unloaded
			// log. Capping to NextSCN itself (not NextSCN-1) would ask LogMiner for a row that
			// only exists in that unloaded file, silently dropping it.
			name:         "capped set bounds to one below the loaded coverage's end, not to NextSCN itself",
			capped:       []*LogFile{{Sequence: 10, Type: "ARCHIVED", NextSCN: 4436937}},
			allOnline:    false,
			dbCurrentSCN: 4440000,
			want:         4436936,
		},
		{
			name:         "capped bound is only applied when it's actually tighter than dbCurrentSCN",
			capped:       []*LogFile{{Sequence: 10, Type: "ARCHIVED", NextSCN: 4436937}},
			allOnline:    false,
			dbCurrentSCN: 4436900, // already below the capped set's own bound
			want:         4436900,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveUpperBound(tt.capped, tt.allOnline, tt.dbCurrentSCN)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCapLogsByCountGrowsToFindTransactionEnd(t *testing.T) {
	// Simulates miningCycleLogCount's adaptive growth: a long-running transaction spans more logs
	// than the configured minimum, so the capped log set stays the same across a cycle and the
	// count must grow until the set advances (mirrors Debezium's CappedLogFileSessionSelector).
	const unit = int64(1000)
	logs := []*LogFile{
		{Sequence: 1, Type: "ARCHIVED", Bytes: unit},
		{Sequence: 2, Type: "ARCHIVED", Bytes: unit},
		{Sequence: 3, Type: "ARCHIVED", Bytes: unit},
		{Sequence: 4, Type: "ONLINE", IsCurrent: true, Bytes: unit},
	}

	firstCapped, firstAllOnline := capLogsByCount(logs, 1, unit)
	require.False(t, firstAllOnline)
	require.Len(t, firstCapped, 1)

	// Re-running with the same log set and count reproduces the same selection - the caller
	// would detect this via sameLogFiles and grow the count.
	sameCapped, _ := capLogsByCount(logs, 1, unit)
	assert.True(t, sameLogFiles(firstCapped, sameCapped))

	grownCapped, grownAllOnline := capLogsByCount(logs, 2, unit)
	assert.False(t, sameLogFiles(firstCapped, grownCapped))
	assert.False(t, grownAllOnline)
	assert.Len(t, grownCapped, 2)
}

func TestSameLogFiles(t *testing.T) {
	tests := []struct {
		name string
		a    []*LogFile
		b    []*LogFile
		want bool
	}{
		{
			name: "identical thread and sequence",
			a:    []*LogFile{{Thread: 1, Sequence: 1}, {Thread: 1, Sequence: 2}},
			b:    []*LogFile{{Thread: 1, Sequence: 1}, {Thread: 1, Sequence: 2}},
			want: true,
		},
		{
			name: "different lengths",
			a:    []*LogFile{{Thread: 1, Sequence: 1}},
			b:    []*LogFile{{Thread: 1, Sequence: 1}, {Thread: 1, Sequence: 2}},
			want: false,
		},
		{
			name: "same length, different sequence",
			a:    []*LogFile{{Thread: 1, Sequence: 1}},
			b:    []*LogFile{{Thread: 1, Sequence: 2}},
			want: false,
		},
		{
			name: "same sequence, different thread",
			a:    []*LogFile{{Thread: 1, Sequence: 1}},
			b:    []*LogFile{{Thread: 2, Sequence: 1}},
			want: false,
		},
		{
			name: "both empty",
			a:    nil,
			b:    nil,
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sameLogFiles(tt.a, tt.b))
		})
	}
}

func TestShouldDeferMiningCycle(t *testing.T) {
	tests := []struct {
		name          string
		currentSCN    uint64
		dbSCN         uint64
		minWindowSize int
		shouldDefer   bool
	}{
		{
			name:          "gap smaller than min window defers the cycle",
			currentSCN:    1000,
			dbSCN:         1005,
			minWindowSize: 100,
			shouldDefer:   true,
		},
		{
			name:          "gap equal to min window does not defer",
			currentSCN:    1000,
			dbSCN:         1100,
			minWindowSize: 100,
			shouldDefer:   false,
		},
		{
			name:          "gap larger than min window does not defer",
			currentSCN:    1000,
			dbSCN:         2000,
			minWindowSize: 100,
			shouldDefer:   false,
		},
		{
			name:          "min window of zero disables the guard",
			currentSCN:    1000,
			dbSCN:         1001,
			minWindowSize: 0,
			shouldDefer:   false,
		},
		{
			name:          "scns equal does not defer (existing guard handles this)",
			currentSCN:    1000,
			dbSCN:         1000,
			minWindowSize: 100,
			shouldDefer:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deferMiningCycle(tt.currentSCN, tt.dbSCN, tt.minWindowSize)
			assert.Equal(t, tt.shouldDefer, got)
		})
	}
}

// TestBasicfileOORInferFromLOBOnlyUpdate covers the exact CI failure scenario where:
//   - A BASICFILE DISABLE STORAGE IN ROW LOB column (OOL_COL) has its LOB_WRITE events
//     arrive before any DML event; all are deferred.
//   - Oracle emits a LOB-only UPDATE for a SecureFile column (SECUREFILE_COL) that does
//     NOT include OOL_COL in its SET clause (BASICFILE OOR columns are absent).
//   - There is no INSERT event in the transaction cache.
//   - At COMMIT time, inferLOBLocator must treat OOL_COL as a BASICFILE OOR candidate
//     from the LOB-only UPDATE (absent from SET = BASICFILE OOR, not a disqualifier).
func TestBasicfileOORInferFromLOBOnlyUpdate(t *testing.T) {
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	pub := &publisherStub{}
	lm := newLogMiner(pub, cache)
	lm.cfg.LOBEnabled = true
	lm.lobColTypes = map[string]string{
		"TESTDB.T.OOL_COL":        "CLOB",
		"TESTDB.T.SECUREFILE_COL": "CLOB",
	}

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 100, Operation: sqlredo.OpStart, TransactionID: "txA",
	}))

	// OOL_COL LOB_WRITEs arrive with no DML events in txnCache yet — all deferred.
	for _, write := range []string{
		" buf_c := 'hello';\n  dbms_lob.write(loc_c, 5, 1, buf_c);",
		" buf_c := 'world';\n  dbms_lob.write(loc_c, 5, 6, buf_c);",
	} {
		require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           101,
			Operation:     sqlredo.OpLobWrite,
			TransactionID: "txA",
			SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
			TableName:     sql.NullString{String: "T", Valid: true},
			SQLRedo:       sql.NullString{String: write, Valid: true},
		}))
	}
	assert.Len(t, lm.pendingLOBWrites["txA"], 2, "OOL_COL LOB_WRITEs should be deferred")

	// Oracle emits a LOB-only UPDATE for SECUREFILE_COL. OOL_COL (BASICFILE OOR) is
	// absent from the SET clause — this is the case that triggered the CI failure.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           102,
		Operation:     sqlredo.OpUpdate,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `update "TESTDB"."T" set "SECUREFILE_COL" = 'X' where "ID" = '42'`, Valid: true},
	}))

	// SECUREFILE_COL gets its real data via SELECT_LOB_LOCATOR + LOB_WRITE.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           103,
		Operation:     sqlredo.OpSelectLobLocator,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `declare lob_1 clob; begin select "SECUREFILE_COL" into lob_1 from "TESTDB"."T" where "ID" = '42';`, Valid: true},
	}))
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           104,
		Operation:     sqlredo.OpLobWrite,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: " buf_c := 'securedata';\n  dbms_lob.write(loc_c, 10, 1, buf_c);", Valid: true},
	}))

	// COMMIT — replay deferred LOB_WRITEs; inferLOBLocator must find OOL_COL as a
	// candidate from the LOB-only UPDATE (absent from SET = BASICFILE OOR, not a skip).
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 200, Operation: sqlredo.OpCommit, TransactionID: "txA",
	}))

	require.Len(t, pub.messages, 1)
	data, ok := pub.messages[0].Data.(map[string]any)
	require.True(t, ok, "Data should be map[string]any")
	assert.Equal(t, "helloworld", data["OOL_COL"], "BASICFILE OOR column should have deferred LOB_WRITEs assembled")
	assert.Equal(t, "securedata", data["SECUREFILE_COL"], "SecureFile column should have its LOB_WRITE data")
	assert.Empty(t, lm.pendingLOBWrites, "no LOB_WRITEs should remain deferred after COMMIT")
}

func newLogMiner(pub replication.ChangePublisher, cache TransactionCache) *LogMiner {
	return &LogMiner{
		publisher:        pub,
		txnCache:         cache,
		dmlParser:        sqlredo.NewParser(),
		log:              service.NewLoggerFromSlog(slog.Default()),
		cfg:              NewDefaultConfig(),
		lobStates:        make(map[sqlredo.TransactionID]*sqlredo.TxnLOBState),
		pendingLOBWrites: make(map[sqlredo.TransactionID][]*sqlredo.RedoEvent),
	}
}

type publisherStub struct{ messages []*replication.MessageEvent }

func (p *publisherStub) Publish(_ context.Context, msg *replication.MessageEvent) error {
	p.messages = append(p.messages, msg)
	return nil
}

func (*publisherStub) Close() {}
