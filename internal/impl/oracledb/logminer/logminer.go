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
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"time"

	goora "github.com/sijms/go-ora/v2/network"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

var (
	// captures the time between DB commit and publish
	publishLatencyMetric = "oracledb_cdc_publish_lag_ns"
	// captures the time from query execution to zero or more rows being returned by LogMiner
	timeToFirstRowMetric = "oracledb_cdc_logminer_time_to_first_row_ns"
	// https://docs.oracle.com/en/error-help/db/ora-01291/
	errCodeMissingLogFile = 1291
	// https://docs.oracle.com/en/error-help/db/ora-01368/
	errCodeRedoLogHeaderMismatch = 1368
)

// LogMiner tracks and streams all change events from the configured change
// tables tracked in tables.
type LogMiner struct {
	cfg          *Config
	tables       []replication.UserTable
	publisher    replication.ChangePublisher
	logCollector *LogFileCollector
	currentSCN   uint64
	windowSize   int
	sessionMgr   *SessionManager

	// state for the log_count windowing strategy (see miningCycleLogCount)
	logCount            int
	previousCappedFiles []*LogFile
	maxRedoLogBytes     int64
	db                  *sql.DB
	dmlParser           *sqlredo.Parser

	// Pre-built query string for LogMiner contents
	logMinerQuery string
	txnCache      TransactionCache

	// Redo logs don't include data types so we have to find lob types up front.
	// ie "TESTDB.PRODUCTS.DESCRIPTION": "NCLOB",
	lobColTypes map[string]string
	// lob types are split between redo log lines, we use lobStates to track them
	// until we have all data to merge into published INSERT or UPDATE event.
	lobStates map[sqlredo.TransactionID]*sqlredo.TxnLOBState
	// pendingLOBWrites holds LOB_WRITE events that arrived before their INSERT
	// (BASICFILE DISABLE STORAGE IN ROW ordering from Oracle LogMiner). They are
	// replayed after the INSERT is buffered so inferLOBLocator can find it.
	pendingLOBWrites map[sqlredo.TransactionID][]*sqlredo.RedoEvent
	// suppresses repeated "caught up" log lines within a single idle stretch
	caughtUpLogged bool

	publishLagMetric     *service.MetricTimer
	timeToFirstRowMetric *service.MetricTimer
	log                  *service.Logger
}

// NewMiner creates a new instance of LogMiner responsible for paging through change events based on the tables param.
// txnCache sets the transaction buffer implementation; pass nil to use the default in-memory cache.
func NewMiner(db *sql.DB, userTables []replication.UserTable, publisher replication.ChangePublisher, cfg *Config, txnCache TransactionCache, metrics *service.Metrics, logger *service.Logger) *LogMiner {
	// Build table filter condition once
	// Transaction control operations (6=START, 7=COMMIT, 36=ROLLBACK) don't have table info
	// and must pass unfiltered. DML (1=INSERT, 2=DELETE, 3=UPDATE) and LOB operations
	// (9=SELECT_LOB_LOCATOR, 10=LOB_WRITE, 11=LOB_TRIM) all carry SEG_OWNER/TABLE_NAME
	// and must be restricted to configured tables to avoid capturing Oracle internal tables.
	var buf strings.Builder
	if len(userTables) > 0 {
		buf.WriteString(" AND (OPERATION_CODE IN (6, 7, 36)")
		// DML and LOB operations carry the real table name — filter by configured tables.
		dmlCodes := "1, 2, 3"
		if cfg.LOBEnabled {
			dmlCodes += ", 9, 10, 11"
		}
		buf.WriteString(" OR (OPERATION_CODE IN (" + dmlCodes + ") AND (") // Filter DML/LOB by table
		for i, t := range userTables {
			if i > 0 {
				buf.WriteString(" OR ")
			}
			fmt.Fprintf(&buf, "(SEG_OWNER = '%s' AND TABLE_NAME = '%s')", strings.ReplaceAll(t.Schema, "'", "''"), strings.ReplaceAll(t.Name, "'", "''"))
		}
		buf.WriteString(")))")
	}
	if cfg.PDBName != "" {
		fmt.Fprintf(&buf, " AND SRC_CON_NAME = '%s'", strings.ReplaceAll(cfg.PDBName, "'", "''"))
	}

	logMinerQuery := fmt.Sprintf(`
		SELECT
			SCN,
			SQL_REDO,
			OPERATION_CODE,
			TABLE_NAME,
			SEG_OWNER,
			TIMESTAMP,
			XID,
			COMMIT_SCN,
			CSF
		FROM V$LOGMNR_CONTENTS
		WHERE SCN > :1 AND SCN <= :2%s
	`, buf.String())

	lm := &LogMiner{
		cfg:                  cfg,
		db:                   db,
		tables:               userTables,
		publisher:            publisher,
		publishLagMetric:     metrics.NewTimer(publishLatencyMetric),
		timeToFirstRowMetric: metrics.NewTimer(timeToFirstRowMetric),
		log:                  logger,

		// logminer specific
		logMinerQuery:    logMinerQuery,
		logCollector:     NewLogFileCollector(),
		sessionMgr:       NewSessionManager(cfg, logger),
		txnCache:         txnCache,
		dmlParser:        sqlredo.NewParser(),
		lobStates:        make(map[sqlredo.TransactionID]*sqlredo.TxnLOBState),
		pendingLOBWrites: make(map[sqlredo.TransactionID][]*sqlredo.RedoEvent),
		windowSize:       cfg.SCNWindowSize,
		logCount:         cfg.LogCountMin,
	}
	if lm.txnCache == nil {
		lm.txnCache = NewInMemoryCache(cfg.MaxTransactionEvents, metrics, logger)
	}
	return lm
}

// ReadChanges streams the change events from LogMiner via a mining cycle.
func (lm *LogMiner) ReadChanges(ctx context.Context, startPos replication.SCN) (resErr error) {
	// Acquire a dedicated connection so that all LogMiner session operations
	// (NLS settings, ADD_LOGFILE, START_LOGMNR, V$LOGMNR_CONTENTS queries) execute
	// on the same underlying Oracle session. Using lm.db directly risks different
	// calls being routed to different pool connections, breaking session-scoped state.
	conn, err := lm.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring dedicated logminer connection: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil && resErr == nil {
			resErr = fmt.Errorf("closing connection: %w", err)
		}
	}()

	if err := replication.ApplyNLSSettings(ctx, conn); err != nil {
		return fmt.Errorf("applying NLS settings for logminer: %w", err)
	}

	// always find all lob columns on start up as redo logs don't include column data types.
	// this also prevents inline lob rows being emitted as events.
	if err := lm.loadLOBColumnTypes(ctx); err != nil {
		return fmt.Errorf("discovering LOB column types: %w", err)
	}

	lm.currentSCN = uint64(startPos)
	lm.log.Infof("Starting streaming change events for %d table(s) beginning from SCN: %d", len(lm.tables), lm.currentSCN)

	defer func() {
		if lm.sessionMgr.IsActive() {
			if err := lm.sessionMgr.EndSession(context.Background(), conn); err != nil {
				lm.log.Errorf("ending logminer session on exit: %v", err)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if caughtUp, err := lm.miningCycle(ctx, conn); err != nil {
				return fmt.Errorf("mining logs: %w", err)
			} else if caughtUp {
				if !lm.caughtUpLogged {
					lm.log.Debugf("Caught up with redo logs, backing off for %s...", lm.cfg.MiningBackoffInterval)
					lm.caughtUpLogged = true
				}
				time.Sleep(lm.cfg.MiningBackoffInterval)
			} else {
				lm.caughtUpLogged = false
				time.Sleep(lm.cfg.MiningInterval)
			}
		}
	}
}

// FindStartPos returns the database's current SCN so that streaming begins from
// the present moment rather than replaying historical redo logs.
func (lm *LogMiner) FindStartPos(ctx context.Context) (replication.SCN, error) {
	var currentPos uint64
	if err := lm.db.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&currentPos); err != nil {
		return 0, fmt.Errorf("querying current SCN from database: %w", err)
	}
	if currentPos == 0 {
		return 0, errors.New("database returned an invalid CURRENT_SCN value (0)")
	}
	return replication.SCN(currentPos), nil
}

// miningCycle runs a single mining iteration using the configured windowing strategy.
func (lm *LogMiner) miningCycle(ctx context.Context, conn *sql.Conn) (caughtUp bool, err error) {
	if lm.cfg.WindowingStrategy == WindowingStrategyLogCount {
		return lm.miningCycleLogCount(ctx, conn)
	}
	return lm.miningCycleSCNRange(ctx, conn)
}

// miningCycleSCNRange is the legacy windowing strategy: the mining session's upper bound is an
// SCN offset (windowSize) from the current position, adapted up/down between min/max bounds
// based on whether the connector is caught up or has a backlog to work through.
func (lm *LogMiner) miningCycleSCNRange(ctx context.Context, conn *sql.Conn) (caughtUp bool, err error) {
	// Get database's current SCN to know our target
	var dbCurrentSCN uint64
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&dbCurrentSCN); err != nil {
		return false, fmt.Errorf("fetching current SCN: %w", err)
	}

	if lm.currentSCN >= dbCurrentSCN {
		return true, nil
	}

	if deferMiningCycle(lm.currentSCN, dbCurrentSCN, lm.cfg.MinSCNWindowSize) {
		return true, nil
	}

	endSCN := dbCurrentSCN
	hitCap := false
	if maxRange := uint64(lm.windowSize); lm.currentSCN+maxRange < dbCurrentSCN {
		endSCN = lm.currentSCN + maxRange
		hitCap = true
	}

	logFiles, err := lm.logCollector.GetLogsBySCNRange(ctx, conn, lm.currentSCN, endSCN)
	if err != nil {
		return false, fmt.Errorf("collecting redo logs for logminer: %w", err)
	}

	tuningHint := fmt.Sprintf(
		"   - Reduce logminer.scn_window_size (current: %d SCN units) to process smaller windows per cycle\n"+
			"   - Decrease logminer.backoff_interval (current: %v)",
		lm.cfg.SCNWindowSize, lm.cfg.MiningBackoffInterval)

	retry, resumeSCN, err := lm.runMiningWindow(ctx, conn, logFiles, lm.currentSCN, endSCN, tuningHint)
	if err != nil {
		return false, err
	}
	if retry {
		// resumeSCN reflects whatever was actually processed (and, for commits, published)
		// before the retry condition hit — advance to it so the next cycle doesn't re-mine
		// and re-publish work already done. See runMiningWindow's doc comment.
		if resumeSCN > lm.currentSCN {
			lm.currentSCN = resumeSCN
		}
		return false, nil
	}

	lm.windowSize = adaptWindowSize(lm.windowSize, hitCap, lm.cfg.MinSCNWindowSize, lm.cfg.MaxSCNWindowSize, lm.cfg.SCNWindowSize)
	lm.currentSCN = endSCN
	return endSCN >= dbCurrentSCN, nil
}

// miningCycleLogCount is the log_count windowing strategy, modeled on Debezium 3.6's approach:
// instead of computing the mining session's upper bound from an adaptive SCN range, it selects a
// minimum number of redo/archive logs (logCount) starting at the current position and derives the
// upper bound directly from the logs selected. When every selected log is the current online log
// (i.e. there's no backlog of completed archive logs to work through), no cap is applied and the
// session mines all the way up to the database's current SCN. If the selected log set doesn't
// change between cycles — meaning a long-running transaction spans more logs than the configured
// minimum — the count grows by one log to make progress toward that transaction's commit; once the
// log set advances again, the count resets back to the configured minimum.
func (lm *LogMiner) miningCycleLogCount(ctx context.Context, conn *sql.Conn) (caughtUp bool, err error) {
	var dbCurrentSCN uint64
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&dbCurrentSCN); err != nil {
		return false, fmt.Errorf("fetching current SCN: %w", err)
	}

	if lm.currentSCN >= dbCurrentSCN {
		return true, nil
	}

	if lm.cfg.LogCountMin > 0 && lm.maxRedoLogBytes == 0 {
		maxBytes, err := lm.logCollector.GetMaxOnlineRedoLogBytes(ctx, conn)
		if err != nil {
			return false, fmt.Errorf("determining maximum online redo log size: %w", err)
		}
		lm.maxRedoLogBytes = maxBytes
	}

	logs, err := lm.logCollector.GetLogsFromSCN(ctx, conn, lm.currentSCN)
	if err != nil {
		return false, fmt.Errorf("collecting redo logs from SCN %d: %w", lm.currentSCN, err)
	}
	if len(logs) == 0 {
		// No logs cover the current position yet (e.g. a log switch is in progress); back off.
		return true, nil
	}

	capped, allOnline := capLogsByCount(logs, lm.logCount, lm.maxRedoLogBytes)
	if lm.cfg.LogCountMin > 0 && lm.previousCappedFiles != nil {
		if sameLogFiles(capped, lm.previousCappedFiles) {
			lm.logCount++
			lm.log.Debugf("Capped log set unchanged since last cycle, growing log count to %d to make progress on a long-running transaction.", lm.logCount)
			capped, allOnline = capLogsByCount(logs, lm.logCount, lm.maxRedoLogBytes)
		} else if lm.logCount > lm.cfg.LogCountMin {
			lm.logCount = lm.cfg.LogCountMin
			lm.log.Debugf("Capped log set advanced, resetting log count to %d.", lm.logCount)
			capped, allOnline = capLogsByCount(logs, lm.logCount, lm.maxRedoLogBytes)
		}
	}
	lm.previousCappedFiles = capped

	endSCN := effectiveUpperBound(capped, allOnline, dbCurrentSCN)

	tuningHint := fmt.Sprintf(
		"   - Decrease logminer.log_count_min (current: %d) to process fewer logs per cycle\n"+
			"   - Decrease logminer.backoff_interval (current: %v)",
		lm.cfg.LogCountMin, lm.cfg.MiningBackoffInterval)

	retry, resumeSCN, err := lm.runMiningWindow(ctx, conn, capped, lm.currentSCN, endSCN, tuningHint)
	if err != nil {
		return false, err
	}
	if retry {
		// resumeSCN reflects whatever was actually processed (and, for commits, published)
		// before the retry condition hit — advance to it so the next cycle doesn't re-mine
		// and re-publish work already done. See runMiningWindow's doc comment.
		if resumeSCN > lm.currentSCN {
			lm.currentSCN = resumeSCN
		}
		return false, nil
	}

	if allOnline {
		// dbCurrentSCN was read before ADD_LOGFILE/START_LOGMNR/the content query ran. If the
		// online log we mined switched out somewhere in that round-trip, dbCurrentSCN may
		// already reflect commits written to a *different*, newer online log we never loaded —
		// LogMiner doesn't error in that case, it just silently returns fewer rows than the
		// ENDSCN implies. Trusting dbCurrentSCN as endSCN and advancing currentSCN to it would
		// then permanently skip whatever landed in that newer log before our next cycle notices
		// it. Re-check the log we actually mined: if it's still the live one, everything up to
		// dbCurrentSCN is guaranteed covered (CURRENT_SCN can't outrun the log currently
		// receiving writes); if it switched out, fall back to its own now-fixed coverage.
		stillCurrent, sealedNextSCN, err := lm.logCollector.CheckLogStillCurrent(ctx, conn, capped[len(capped)-1])
		if err != nil {
			return false, err
		}
		if !stillCurrent && sealedNextSCN > lm.currentSCN && sealedNextSCN-1 < endSCN {
			endSCN = sealedNextSCN - 1
		}
	}

	lm.currentSCN = endSCN
	return endSCN >= dbCurrentSCN, nil
}

// runMiningWindow starts (or extends) a LogMiner session over logFiles bounded by
// [startSCN, endSCN], then queries and processes V$LOGMNR_CONTENTS for that same range.
// Restarting the session on every cycle with explicit SCN bounds is required because Oracle's
// START_LOGMNR with ENDSCN=0 freezes the session's view at session start time, making events
// written after session start invisible; per-window restart with explicit endSCN ensures all
// events in [startSCN, endSCN] are visible.
//
// retry reports whether the caller should retry rather than treat this as a fatal error — a
// redo log being recycled mid-window is a recoverable, expected condition under fast log
// switching. resumeSCN is only meaningful when retry is true: it is the SCN of the last event
// that was actually handed to processRedoEvent before the retry condition hit, which the caller
// MUST advance to (never re-mine from the original startSCN unchanged) if it's past startSCN.
//
// This distinction matters because processRedoEvent's COMMIT case has a real, irreversible side
// effect — publishing events downstream. If the query got partway through [startSCN, endSCN],
// published some commits, and only then hit ORA-01368, blindly retrying the original startSCN
// would re-mine and re-publish everything already published. Worse, under sustained load each
// such retry's endSCN is computed fresh (a growing dbCurrentSCN or wider log selection), so a
// naive "same startSCN, bigger window" retry compounds: it takes longer, giving the live log
// more time to switch again mid-query, triggering another retry with an even bigger window — a
// self-reinforcing loop that was observed in practice to redeliver the same events 2x+ under a
// sustained ~20 MiB/s write rate. Resuming from resumeSCN keeps every retry attempt bounded to
// only the unprocessed remainder.
func (lm *LogMiner) runMiningWindow(ctx context.Context, conn *sql.Conn, logFiles []*LogFile, startSCN, endSCN uint64, tuningHint string) (retry bool, resumeSCN uint64, err error) {
	if err := lm.startSessionWithLogFiles(ctx, conn, startSCN, endSCN, logFiles); err != nil {
		var oraErr *goora.OracleError
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeMissingLogFile {
			//nolint:staticcheck
			return false, 0, fmt.Errorf("preparing logs and starting session at position %d: %w\n\n"+
				"This error indicates archived redo logs have been purged before LogMiner could process them.\n"+
				"This typically happens when processing takes longer than Oracle's log retention period.\n\n"+
				"To fix this issue:\n"+
				"1. Increase Oracle's archived log retention using RMAN:\n"+
				"   CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 7 DAYS;\n\n"+
				"2. Improve processing performance:\n"+
				"%s\n"+
				"   - Increase input batching.count for better throughput\n"+
				"   - Use faster output (e.g., drop: {} for benchmarking)\n\n"+
				"3. Restart the connector from the current database SCN to skip missing logs:\n"+
				"   Note: This will result in data loss for events in the purged logs, so a snapshot may be required.",
				startSCN, err, tuningHint)
		}
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeRedoLogHeaderMismatch {
			lm.log.Debugf("ORA-01368: redo log sequence recycled before session could start (SCN range %d–%d); the log will be available as an archived log on next cycle", startSCN, endSCN)
			// No rows were read yet — nothing was published, so the original startSCN is
			// still exactly the right resume point.
			return true, startSCN, nil
		}
		return false, 0, fmt.Errorf("preparing logs and starting session at position %d: %w", startSCN, err)
	}

	// Query and process redoEvents from V$LOGMNR_CONTENTS
	// The session is already active, just query it
	lastSCN, err := lm.queryLogMinerContents(ctx, conn, startSCN, endSCN, lm.processRedoEvent)
	if err != nil {
		var oraErr *goora.OracleError
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeRedoLogHeaderMismatch {
			lm.log.Debugf("ORA-01368: redo log sequence recycled mid-query (SCN range %d–%d); retrying from SCN %d — archived log will be used on next cycle", startSCN, endSCN, lastSCN)
			return true, lastSCN, nil
		}
		return false, 0, fmt.Errorf("querying logminer contents between %d and %d: %w", startSCN, endSCN, err)
	}

	return false, 0, nil
}

// processRedoEvent buffers emitted events until a commit or rollback event is processed at which
// point the buffer can be flushed to the Connect pipeline or dropped.
func (lm *LogMiner) processRedoEvent(ctx context.Context, redoEvent *sqlredo.RedoEvent) error {
	switch redoEvent.Operation {
	case sqlredo.OpStart:
		// Transaction started
		if err := lm.txnCache.StartTransaction(ctx, redoEvent.TransactionID, redoEvent.SCN); err != nil {
			return fmt.Errorf("starting transaction %s: %w", redoEvent.TransactionID, err)
		}

	case sqlredo.OpInsert, sqlredo.OpUpdate, sqlredo.OpDelete:
		// SQL_REDO should always be present for DML operations. If not, it's likely a temporary
		// table (Oracle doesn't generate redo for these) or an unsupported operation.
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			lm.log.Warnf("Skipping DML event with no SQL_REDO (operation=%s, table=%s.%s, scn=%d, txn=%s) - likely temporary table or unsupported operation",
				redoEvent.Operation, redoEvent.SchemaName.String, redoEvent.TableName.String, redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}

		// Parse sql insert/update/delete sql statements into key/value object
		event, err := lm.dmlParser.RedoEventToDMLEvent(redoEvent)
		if err != nil {
			lm.log.Debugf("failed to parse SQL_REDO (scn=%d, op=%s, table=%s.%s, txn=%s): %s",
				redoEvent.SCN, redoEvent.Operation, redoEvent.SchemaName.String, redoEvent.TableName.String, redoEvent.TransactionID, redoEvent.SQLRedo.String)
			return fmt.Errorf("parsing sql redo event into dml event: %w", err)
		}

		if err := lm.txnCache.AddEvent(ctx, redoEvent.TransactionID, redoEvent.SCN, &event); err != nil {
			return fmt.Errorf("adding event to transaction %s: %w", redoEvent.TransactionID, err)
		}

	case sqlredo.OpSelectLobLocator:
		if !lm.cfg.LOBEnabled {
			return nil
		}
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			lm.log.Warnf("Skipping SELECT_LOB_LOCATOR with no SQL_REDO (scn=%d, txn=%s)", redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}
		info, err := sqlredo.ParseSelectLobLocator(redoEvent.SQLRedo.String)
		if err != nil {
			lm.log.Warnf("Failed to parse SELECT_LOB_LOCATOR SQL (scn=%d, txn=%s): %v\nSQL: %.500s", redoEvent.SCN, redoEvent.TransactionID, err, redoEvent.SQLRedo.String)
			return nil
		}
		// Resolve LOB type from the schema cache populated at startup.
		colKey := fmt.Sprintf("%s.%s.%s", info.Schema, info.Table, info.Column)
		lobType := lm.lobColTypes[strings.ToUpper(colKey)] // "CLOB", "BLOB", "NCLOB", or "" if unknown

		state := lm.getOrCreateLOBState(redoEvent.TransactionID)
		key := sqlredo.LobKey{
			Schema:   info.Schema,
			Table:    info.Table,
			Column:   info.Column,
			PKString: sqlredo.FormatPKString(info.PKValues),
		}
		if _, exists := state.Accumulators[key]; !exists {
			state.Accumulators[key] = &sqlredo.LobAccumulator{
				Schema:   info.Schema,
				Table:    info.Table,
				Column:   info.Column,
				PKValues: info.PKValues,
				IsBinary: lobType == "BLOB",
			}
		}
		state.ActiveKey = &key

	case sqlredo.OpLobTrim:
		if !lm.cfg.LOBEnabled {
			return nil
		}
		// LOB_TRIM (op 11) comes in two forms depending on Oracle LOB type:
		//
		// Form A — SELECT "COL" INTO ... FROM "SCHEMA"."TABLE" WHERE ...
		//   Emitted for certain LOB types (e.g. out-of-line SecureFile) without a preceding
		//   SELECT_LOB_LOCATOR. In this case LOB_TRIM itself must establish the accumulator.
		//
		// Form B — dbms_lob.trim(loc_b, N)
		//   Emitted when a SELECT_LOB_LOCATOR has already established the active key.
		//   No schema/table/column info is present. The accumulator is left untouched
		//   regardless of N — see the inline comment below for the rationale.
		//
		//   When N>0 and no fragments have been accumulated, a warning is emitted because
		//   Oracle intends to keep the first N bytes/chars of the pre-existing LOB, which
		//   we do not hold. In the common SecureFile full-rewrite path LOB_WRITE(s) precede
		//   LOB_TRIM and the assembled length equals N, so no data is lost in practice.
		if redoEvent.SQLRedo.Valid && redoEvent.SQLRedo.String != "" {
			if info, err := sqlredo.ParseSelectLobLocator(redoEvent.SQLRedo.String); err == nil {
				// Form A: establish (or reset) the accumulator for this LOB column.
				colKey := fmt.Sprintf("%s.%s.%s", info.Schema, info.Table, info.Column)
				lobType := lm.lobColTypes[strings.ToUpper(colKey)]
				state := lm.getOrCreateLOBState(redoEvent.TransactionID)
				key := sqlredo.LobKey{
					Schema:   info.Schema,
					Table:    info.Table,
					Column:   info.Column,
					PKString: sqlredo.FormatPKString(info.PKValues),
				}
				state.Accumulators[key] = &sqlredo.LobAccumulator{
					Schema:   info.Schema,
					Table:    info.Table,
					Column:   info.Column,
					PKValues: info.PKValues,
					IsBinary: lobType == "BLOB",
				}
				state.ActiveKey = &key
				return nil
			}
		}
		// Form B: LOB_TRIM carries no schema/table/column info — the active key was
		// already established by SELECT_LOB_LOCATOR. Oracle may emit LOB_TRIM before
		// LOB_WRITE (BASICFILE "clear then write") or after (SecureFile "write then
		// finalize"). In both cases the accumulator should be left untouched:
		//   - Before LOB_WRITE: accumulator is empty anyway, so there is nothing to clear.
		//   - After LOB_WRITE:  fragments are already accumulated; clearing them would
		//     destroy the data before commit.
		state, exists := lm.lobStates[redoEvent.TransactionID]
		if !exists || state.ActiveKey == nil {
			return nil
		}
		if redoEvent.SQLRedo.Valid && redoEvent.SQLRedo.String != "" {
			if trimLen, err := sqlredo.ParseLobTrim(redoEvent.SQLRedo.String); err == nil && trimLen > 0 {
				// Warn only for the blatant case: N>0 with no fragments at all, meaning
				// the existing LOB prefix is preserved but we have nothing to emit.
				// Two adjacent cases (N < total written bytes, or N > total written bytes
				// with M>0) also produce an assembled value that does not exactly match N,
				// but Assemble() is not truncated to N here. This is an intentional
				// tradeoff: SecureFile full-rewrite UPDATEs (the common path) always write
				// all bytes then trim to the exact final length, so assembled==N in
				// practice. Partial-update patterns are not supported by this path.
				if acc := state.Accumulators[*state.ActiveKey]; acc != nil && len(acc.Fragments) == 0 {
					lm.log.Warnf("LOB_TRIM to non-zero length %d with no prior LOB_WRITE (scn=%d, txn=%s): assembled value may be incomplete", trimLen, redoEvent.SCN, redoEvent.TransactionID)
				}
			}
		}

	case sqlredo.OpLobWrite:
		if !lm.cfg.LOBEnabled {
			return nil
		}
		state, exists := lm.lobStates[redoEvent.TransactionID]
		if !exists || state.ActiveKey == nil {
			if !lm.inferLOBLocator(ctx, redoEvent) {
				// INSERT may arrive later in the same LogMiner batch (BASICFILE
				// DISABLE STORAGE IN ROW ordering). Defer and replay after DML.
				lm.log.Debugf("LOB_WRITE before INSERT (scn=%d, txn=%s): deferring", redoEvent.SCN, redoEvent.TransactionID)
				lm.pendingLOBWrites[redoEvent.TransactionID] = append(lm.pendingLOBWrites[redoEvent.TransactionID], redoEvent)
				return nil
			}
			state = lm.lobStates[redoEvent.TransactionID]
		}
		acc := state.Accumulators[*state.ActiveKey]
		if acc == nil {
			lm.log.Warnf("LOB_WRITE has active key but no accumulator (scn=%d, txn=%s)", redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			return nil
		}
		// NCLOB LOB_WRITE SQL delivers data as a plain string literal (same as CLOB),
		// not as HEXTORAW. Only BLOB uses binary/hex encoding.
		writeInfo, err := sqlredo.ParseLobWrite(redoEvent.SQLRedo.String, acc.IsBinary)
		if err != nil {
			lm.log.Warnf("Failed to parse LOB_WRITE SQL (scn=%d, txn=%s): %v\nSQL: %.500s", redoEvent.SCN, redoEvent.TransactionID, err, redoEvent.SQLRedo.String)
			return nil
		}
		acc.AddFragment(writeInfo.Offset, writeInfo.Data)

	case sqlredo.OpCommit:
		// Flush all buffered events for given transaction ID
		txn, err := lm.txnCache.GetTransaction(ctx, redoEvent.TransactionID)
		if err != nil {
			return fmt.Errorf("fetching transaction %s on commit: %w", redoEvent.TransactionID, err)
		}
		if txn != nil {
			safeCheckpointSCN := redoEvent.SCN

			// If other transactions are still open, we must not advance the
			// checkpoint past their start SCN - 1. Doing so would cause their
			// already-seen DML events to be skipped on restart (the query resumes
			// from SCN > checkpoint). We subtract 1 because the query is exclusive.
			if lowestOpenSCN := lm.txnCache.LowWatermarkSCN(redoEvent.TransactionID); lowestOpenSCN != math.MaxUint64 && lowestOpenSCN > 0 {
				if lowestOpenSCN-1 < safeCheckpointSCN {
					safeCheckpointSCN = lowestOpenSCN - 1
				}
			}

			if lm.cfg.LOBEnabled {
				// Replay deferred LOB_WRITEs (BASICFILE DISABLE STORAGE IN ROW) before
				// merging. At commit time, SELECT_LOB_LOCATOR has already claimed all
				// SecureFile LOB columns, so inferLOBLocator can identify the unclaimed
				// BASICFILE column by excluding columns that already have accumulators.
				if err := lm.replayDeferredLOBWrites(ctx, redoEvent.TransactionID); err != nil {
					return err
				}

				// Merge any accumulated LOB data into DML events before publishing.
				if state, ok := lm.lobStates[redoEvent.TransactionID]; ok {
					unmerged := sqlredo.MergeLOBsIntoDMLEvents(state, txn.Events, lm.log)
					// Synthesize UPDATE events for LOB accumulators that had no matching DML
					// event. This handles Oracle SecureFile out-of-row LOBs where Oracle does
					// not emit a DML UPDATE in LogMiner — only SELECT_LOB_LOCATOR + LOB_WRITE
					// + LOB_TRIM operations are recorded.
					//
					// The synthesized event is intentionally sparse: Data contains only the
					// LOB column(s) and OldValues contains only the PK columns extracted from
					// the SELECT_LOB_LOCATOR WHERE clause. Other row columns are not available
					// from redo alone. Carrying over values from a prior event in the same
					// transaction is not possible here: MergeLOBsIntoDMLEvents merges into any
					// matching INSERT (Pass 1) or PK-bearing UPDATE (Pass 2) before returning
					// an accumulator as unmerged, so by definition no full-row DML event for
					// this row exists in the transaction. Downstream consumers should treat a
					// sparse UPDATE (OldValues containing only PK columns) as a LOB-column-only
					// change with no information about other columns.
					for _, acc := range unmerged {
						assembled := acc.Assemble()
						if assembled == nil {
							continue
						}
						synthetic := &sqlredo.DMLEvent{
							Operation:     sqlredo.OpUpdate,
							Schema:        acc.Schema,
							Table:         acc.Table,
							Data:          map[string]any{acc.Column: assembled},
							OldValues:     acc.PKValues,
							TransactionID: redoEvent.TransactionID,
							Timestamp:     redoEvent.Timestamp,
						}
						txn.Events = append(txn.Events, synthetic)
						lm.log.Debugf("LOB merge: synthesized UPDATE for %s.%s.%s (pks=%v, fragments=%d)", acc.Schema, acc.Table, acc.Column, acc.PKValues, len(acc.Fragments))
					}
				}
			}

			// Build a set of schema.table pairs that have an INSERT in this transaction.
			// Used below to detect and suppress Oracle-internal LOB-initialisation UPDATEs.
			insertTables := make(map[string]struct{})
			for _, ev := range txn.Events {
				if ev.Operation == sqlredo.OpInsert {
					insertTables[ev.Schema+"."+ev.Table] = struct{}{}
				}
			}

			if lm.cfg.LOBEnabled {
				// Pre-pass: for each LOB-only UPDATE that accompanies an INSERT in this transaction,
				// merge the actual LOB values into the INSERT before we start publishing.
				//
				// Oracle omits LOB columns from the INSERT SQL_REDO entirely and instead emits a
				// separate UPDATE whose SET clause carries the real LOB data. We must propagate
				// those values into the INSERT event before suppressing the UPDATE.
				for _, dmlEvent := range txn.Events {
					if dmlEvent.Operation != sqlredo.OpUpdate || !lm.isLOBOnlyEvent(dmlEvent) {
						continue
					}
					if _, hasInsert := insertTables[dmlEvent.Schema+"."+dmlEvent.Table]; !hasInsert {
						continue
					}
					sqlredo.MergeInlineLOBValues(dmlEvent.Data, dmlEvent.Schema, dmlEvent.Table, dmlEvent.OldValues, txn.Events, lm.log)
				}
			}

			for _, dmlEvent := range txn.Events {
				// Suppress Oracle-internal LOB-initialisation UPDATEs. Their LOB values have
				// already been merged into the corresponding INSERT by the pre-pass above.
				if dmlEvent.Operation == sqlredo.OpUpdate && lm.isLOBOnlyEvent(dmlEvent) {
					if _, hasInsert := insertTables[dmlEvent.Schema+"."+dmlEvent.Table]; hasInsert {
						lm.log.Debugf("suppressing LOB-only UPDATE for %s.%s — values merged into INSERT", dmlEvent.Schema, dmlEvent.Table)
						continue
					}
				}
				msg := toMessageEvent(dmlEvent, redoEvent.SCN, safeCheckpointSCN, redoEvent.Timestamp)
				if err := lm.publisher.Publish(ctx, msg); err != nil {
					return fmt.Errorf("publishing event with SCN '%d': %w", redoEvent.SCN, err)
				}
				lm.publishLagMetric.Timing(time.Since(redoEvent.Timestamp).Nanoseconds())
			}

			if err := lm.txnCache.CommitTransaction(ctx, redoEvent.TransactionID); err != nil {
				return fmt.Errorf("committing transaction %s: %w", redoEvent.TransactionID, err)
			}
		}

		// Always clean up lobStates on commit, including for transactions discarded by
		// the cache (GetTransaction returns nil when MaxTransactionEvents is exceeded).
		// Without this, LOB events that bypass the cache continue to accumulate in
		// lobStates and are never freed.
		if lm.cfg.LOBEnabled {
			delete(lm.lobStates, redoEvent.TransactionID)
			if pending := lm.pendingLOBWrites[redoEvent.TransactionID]; len(pending) > 0 {
				for _, p := range pending {
					lm.log.Warnf("Dropping deferred LOB_WRITE on commit: txn=%s scn=%d schema=%s table=%s sql=%.200s",
						redoEvent.TransactionID, p.SCN, p.SchemaName.String, p.TableName.String, p.SQLRedo.String)
				}
				delete(lm.pendingLOBWrites, redoEvent.TransactionID)
			}
		}

	case sqlredo.OpRollback:
		// Discard all buffered events for this transaction
		if lm.cfg.LOBEnabled {
			delete(lm.lobStates, redoEvent.TransactionID)
			delete(lm.pendingLOBWrites, redoEvent.TransactionID)
		}
		if err := lm.txnCache.RollbackTransaction(ctx, redoEvent.TransactionID); err != nil {
			return fmt.Errorf("rolling back transaction %s: %w", redoEvent.TransactionID, err)
		}
	}

	return nil
}

func (lm *LogMiner) loadLOBColumnTypes(ctx context.Context) (resErr error) {
	lm.lobColTypes = make(map[string]string)
	if len(lm.tables) == 0 {
		return nil
	}

	// ALL_TAB_COLUMNS must run in PDB context in CDB mode — the LogMiner conn is
	// pinned to CDB$ROOT where PDB tables are not visible via ALL_TAB_COLUMNS.
	// Use a separate connection and switch context if needed.
	catalogConn, err := lm.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection for LOB column discovery: %w", err)
	}
	defer func() {
		if err := catalogConn.Close(); err != nil && resErr == nil {
			resErr = fmt.Errorf("closing catalog connection: %w", err)
		}
	}()

	if lm.cfg.PDBName != "" {
		// can't use parameterized queries here but we've validated on input.
		if _, err := catalogConn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+lm.cfg.PDBName); err != nil {
			return fmt.Errorf("switching session to PDB %s for LOB column discovery: %w", lm.cfg.PDBName, err)
		}
		defer func() {
			if _, err := catalogConn.ExecContext(context.Background(), "ALTER SESSION SET CONTAINER = CDB$ROOT"); err != nil && resErr == nil {
				resErr = fmt.Errorf("switching session back to root container: %w", err)
			}
		}()
	}

	var (
		qb     strings.Builder
		qbArgs []any
	)
	qb.WriteString(`SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE DATA_TYPE IN ('CLOB', 'BLOB', 'NCLOB') AND (`)
	for i, t := range lm.tables {
		if i > 0 {
			qb.WriteString(" OR ")
		}
		fmt.Fprintf(&qb, "(OWNER = '%s' AND TABLE_NAME = '%s')",
			strings.ReplaceAll(strings.ToUpper(t.Schema), "'", "''"),
			strings.ReplaceAll(strings.ToUpper(t.Name), "'", "''"))
	}
	qb.WriteString(")")

	rows, err := catalogConn.QueryContext(ctx, qb.String(), qbArgs...)
	if err != nil {
		return fmt.Errorf("querying LOB column types: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			lm.log.Errorf("closing rows: %v", err)
		}
	}()

	for rows.Next() {
		var owner, tableName, columnName, dataType string
		if err := rows.Scan(&owner, &tableName, &columnName, &dataType); err != nil {
			return fmt.Errorf("scanning LOB column type row: %w", err)
		}
		// example: "TESTDB.PRODUCTS.DESCRIPTION" : "CLOB"
		k := fmt.Sprintf("%s.%s.%s", owner, tableName, columnName)
		lm.lobColTypes[k] = dataType
	}

	return rows.Err()
}

// replayDeferredLOBWrites replays LOB_WRITE events that were buffered because
// their INSERT had not yet arrived. Called after each DML event is added to the
// transaction cache so that inferLOBLocator can now find the INSERT.
func (lm *LogMiner) replayDeferredLOBWrites(ctx context.Context, txnID sqlredo.TransactionID) error {
	pending := lm.pendingLOBWrites[txnID]
	if len(pending) == 0 {
		return nil
	}
	lm.log.Debugf("replayDeferredLOBWrites: replaying %d LOB_WRITE(s) for txn %s", len(pending), txnID)
	// Clear before replaying so re-buffering during the loop appends to a fresh slice.
	delete(lm.pendingLOBWrites, txnID)
	// Clear ActiveKey so inferLOBLocator is invoked for the first deferred write.
	// The prior SELECT_LOB_LOCATOR may have left ActiveKey pointing at a SecureFile
	// column; without this reset, deferred LOB_WRITEs would land on that column
	// instead of the unclaimed BASICFILE out-of-row column.
	if state, ok := lm.lobStates[txnID]; ok {
		state.ActiveKey = nil
	}
	for _, ev := range pending {
		if err := lm.processRedoEvent(ctx, ev); err != nil {
			return err
		}
	}
	if reDeferred := len(lm.pendingLOBWrites[txnID]); reDeferred > 0 {
		lm.log.Warnf("replayDeferredLOBWrites: %d LOB_WRITE(s) re-deferred after replay for txn %s — inferLOBLocator still failing", reDeferred, txnID)
	}
	return nil
}

func (lm *LogMiner) getOrCreateLOBState(txnID sqlredo.TransactionID) *sqlredo.TxnLOBState {
	if state, ok := lm.lobStates[txnID]; ok {
		return state
	}

	s := sqlredo.NewTxnLOBState()
	lm.lobStates[txnID] = s
	return s
}

// isLOBOnlyEvent reports whether every column in ev.Data is a known LOB column.
// This identifies Oracle's internal LOB-initialisation UPDATE events, which carry
// only LOB column values and should be suppressed when a matching INSERT already
// exists in the same transaction.
func (lm *LogMiner) isLOBOnlyEvent(ev *sqlredo.DMLEvent) bool {
	if len(ev.Data) == 0 {
		return false
	}
	for col := range ev.Data {
		key := strings.ToUpper(ev.Schema + "." + ev.Table + "." + col)
		if _, exists := lm.lobColTypes[key]; !exists {
			return false
		}
	}
	return true
}

// inferLOBLocator attempts to create a LOB locator for a LOB_WRITE event that
// arrived without a preceding SELECT_LOB_LOCATOR. This happens with BASICFILE
// out-of-line LOBs where Oracle does not emit locator events in LogMiner.
//
// The method searches backward through the transaction's buffered DML events for
// a LOB-only UPDATE or INSERT that can act as an anchor for the LOB data.
// Returns true if a locator was successfully created.
func (lm *LogMiner) inferLOBLocator(ctx context.Context, event *sqlredo.RedoEvent) bool {
	if !event.SchemaName.Valid || !event.TableName.Valid {
		return false
	}
	schema := event.SchemaName.String
	table := event.TableName.String
	if schema == "" || table == "" {
		return false
	}

	txn, err := lm.txnCache.GetTransaction(ctx, event.TransactionID)
	if err != nil {
		lm.log.Errorf("Failed to get transaction %s for LOB locator inference: %v", event.TransactionID, err)
		return false
	}
	if txn == nil {
		lm.log.Debugf("inferLOBLocator: txn %s not in cache (scn=%d, schema=%s, table=%s) — no DML events yet",
			event.TransactionID, event.SCN, schema, table)
		return false
	}

	prefix := strings.ToUpper(schema + "." + table + ".")

	// claimedCols holds LOB column names that already have an accumulator for this
	// schema.table, regardless of PKString. At commit time these are columns
	// claimed by SELECT_LOB_LOCATOR.
	var (
		claimedCols           = make(map[string]struct{})
		emptyClaimedKeys      = make(map[string]sqlredo.LobKey)
		claimedFragmentCounts = make(map[string]int)
	)
	if existingState := lm.lobStates[event.TransactionID]; existingState != nil {
		for k, acc := range existingState.Accumulators {
			if k.Schema == schema && k.Table == table {
				claimedCols[k.Column] = struct{}{}
				claimedFragmentCounts[k.Column] = len(acc.Fragments)
				if len(acc.Fragments) == 0 {
					emptyClaimedKeys[k.Column] = k
				}
			}
		}
	}
	{
		claimed := make([]string, 0, len(claimedCols))
		for c, n := range claimedFragmentCounts {
			claimed = append(claimed, fmt.Sprintf("%s(%d)", c, n))
		}
		empty := make([]string, 0, len(emptyClaimedKeys))
		for c := range emptyClaimedKeys {
			empty = append(empty, c)
		}
		lm.log.Debugf("inferLOBLocator: claimedCols=%v emptyClaimedKeys=%v (txn=%s, scn=%d, table=%s.%s)",
			claimed, empty, event.TransactionID, event.SCN, schema, table)
	}

	for i := len(txn.Events) - 1; i >= 0; i-- {
		ev := txn.Events[i]
		if ev.Schema != schema || ev.Table != table {
			continue
		}

		var pkValues map[string]any
		switch {
		case ev.Operation == sqlredo.OpUpdate && lm.isLOBOnlyEvent(ev):
			pkValues = ev.OldValues
		case ev.Operation == sqlredo.OpInsert:
			// Use the INSERT's non-LOB columns as the PK identifier so that
			// MergeLOBsIntoDMLEvents can still match this INSERT after one of
			// its LOB columns has been overwritten with the assembled value
			// (important when an INSERT has multiple out-of-line LOBs).
			pkValues = make(map[string]any, len(ev.Data))
			for col, val := range ev.Data {
				if _, isLOB := lm.lobColTypes[prefix+strings.ToUpper(col)]; isLOB {
					continue
				}
				pkValues[col] = val
			}
		default:
			continue
		}

		pkString := sqlredo.FormatPKString(pkValues)
		{
			evDataCols := make([]string, 0, len(ev.Data))
			for c := range ev.Data {
				evDataCols = append(evDataCols, c)
			}
			lm.log.Debugf("inferLOBLocator: examining event op=%s nDataCols=%d dataCols=%v (txn=%s, scn=%d)",
				ev.Operation, len(ev.Data), evDataCols, event.TransactionID, event.SCN)
		}

		// Candidate LOB columns are those:
		//   - not already claimed by SELECT_LOB_LOCATOR (tracked in claimedCols)
		//   - absent from ev.Data: INSERT omits BASICFILE OOR columns; LOB-only UPDATE
		//     omits them from its SET clause (they never appear there for BASICFILE OOR)
		//   - present with nil (Oracle writes NULL in INSERT SQL_REDO for out-of-row LOBs)
		//   - present with an empty []byte (EMPTY_CLOB()/EMPTY_BLOB() placeholder)
		for k, lobType := range lm.lobColTypes {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			col := k[len(prefix):]
			// Skip columns already claimed by SELECT_LOB_LOCATOR, unless the
			// accumulator has no fragments yet — meaning SELECT_LOB_LOCATOR arrived
			// after INSERT but the LOB_WRITE events arrived before INSERT and are
			// sitting in the deferred queue. Route them to the existing accumulator.
			if _, claimed := claimedCols[col]; claimed {
				if existingKey, hasEmptyAcc := emptyClaimedKeys[col]; hasEmptyAcc {
					state := lm.getOrCreateLOBState(event.TransactionID)
					state.ActiveKey = &existingKey
					lm.log.Debugf("Inferred LOB locator for %s.%s.%s from empty SELECT_LOB_LOCATOR accumulator (txn=%s)",
						schema, table, col, event.TransactionID)
					return true
				}
				lm.log.Debugf("inferLOBLocator: skip %s.%s.%s — claimed with %d fragment(s) (txn=%s)",
					schema, table, col, claimedFragmentCounts[col], event.TransactionID)
				continue
			}
			val, present := ev.Data[col]
			switch {
			case present:
				// nil means Oracle wrote NULL in INSERT SQL_REDO for this LOB column
				// (BASICFILE DISABLE STORAGE IN ROW). Treat it as a valid candidate.
				if val != nil {
					if b, ok := val.([]byte); !ok || len(b) != 0 {
						lm.log.Debugf("inferLOBLocator: skip %s.%s.%s — INSERT value type=%T val=%.40v (txn=%s)",
							schema, table, col, val, val, event.TransactionID)
						continue
					}
				}
			case ev.Operation != sqlredo.OpInsert:
				// Column absent from a LOB-only UPDATE.
			}

			lm.log.Debugf("inferLOBLocator: CANDIDATE %s.%s.%s present=%v val=%T (txn=%s)",
				schema, table, col, present, val, event.TransactionID)

			key := sqlredo.LobKey{
				Schema:   schema,
				Table:    table,
				Column:   col,
				PKString: pkString,
			}

			// Defer state creation until we have a match to avoid leaking
			// empty TxnLOBState entries when inference fails.
			state := lm.getOrCreateLOBState(event.TransactionID)
			if _, exists := state.Accumulators[key]; exists {
				lm.log.Debugf("inferLOBLocator: skip %s.%s.%s — accumulator already exists for pkString=%q (txn=%s)",
					schema, table, col, pkString, event.TransactionID)
				continue
			}

			state.Accumulators[key] = &sqlredo.LobAccumulator{
				Schema:   schema,
				Table:    table,
				Column:   col,
				PKValues: pkValues,
				IsBinary: lobType == "BLOB",
			}
			state.ActiveKey = &key

			lm.log.Debugf("Inferred LOB locator for %s.%s.%s from %s (txn=%s)",
				schema, table, col, ev.Operation, event.TransactionID)
			return true
		}
	}

	// Log why inference failed: how many events we searched and how many LOB columns we know about.
	var eventsForTable int
	for _, ev := range txn.Events {
		if ev.Schema == schema && ev.Table == table {
			eventsForTable++
		}
	}
	var knownLOBCols []string
	for k := range lm.lobColTypes {
		if strings.HasPrefix(k, prefix) {
			knownLOBCols = append(knownLOBCols, k)
		}
	}
	lm.log.Debugf("inferLOBLocator: no match for %s.%s (txn=%s, scn=%d): txnEvents=%d, eventsForTable=%d, knownLOBCols=%v",
		schema, table, event.TransactionID, event.SCN, len(txn.Events), eventsForTable, knownLOBCols)
	return false
}

// queryLogMinerContents queries and processes V$LOGMNR_CONTENTS for [startSCN, endSCN]. It always
// returns lastSCN — a safe resume point — even when err is non-nil, defaulting to startSCN if
// nothing was processed. The caller uses this on a retryable error: since processEvent's COMMIT
// case publishes downstream with real side effects, re-querying from startSCN after partial
// progress would re-publish already published events, whereas re-querying from lastSCN (via the
// query's "SCN > :1" exclusive lower bound) skips only what was already handled.
//
// lastSCN only ever advances to an SCN once every row at that SCN has been seen — V$LOGMNR_CONTENTS
// commonly has multiple rows sharing one SCN (several row changes within the same redo boundary),
// and rows are returned in SCN order, so seeing a strictly larger SCN is what confirms the previous
// one is exhausted. Advancing eagerly to the SCN of whichever row was processed right before a
// mid-query error would risk a retry's "SCN > lastSCN" query skipping unprocessed siblings still at
// that exact SCN — silent, permanent loss rather than the harmless occasional re-publish this
// function is willing to accept instead.
func (lm *LogMiner) queryLogMinerContents(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64, processEvent func(context.Context, *sqlredo.RedoEvent) error) (lastSCN uint64, err error) {
	lastSCN = startSCN
	if len(lm.tables) == 0 {
		return lastSCN, nil
	}

	// Use the pre-built query from initialization
	queryStart := time.Now()
	rows, err := conn.QueryContext(ctx, lm.logMinerQuery, startSCN, endSCN)
	if err != nil {
		return lastSCN, fmt.Errorf("querying logminer: %w", err)
	}
	defer rows.Close()

	var (
		pending  *sqlredo.RedoEvent // accumulates CSF continuation fragments
		firstRow = true
		// lastProcessedSCN is the SCN of the most recently fully-processed event; it is only
		// promoted to lastSCN once a strictly larger SCN is observed (see doc comment above).
		lastProcessedSCN  uint64
		haveLastProcessed bool
	)
	for rows.Next() {
		if firstRow {
			elapsed := time.Since(queryStart)
			lm.timeToFirstRowMetric.Timing(elapsed.Nanoseconds())
			lm.log.Debugf("LogMiner query returned first row after %s (scn=%d to %d)", elapsed, startSCN, endSCN)
			firstRow = false
		}
		event := &sqlredo.RedoEvent{}
		var (
			commitSCN sql.NullInt64 // COMMIT_SCN can be NULL for uncommitted transactions
			csf       int64         // Continuation SQL Flag: 1 = more SQL in next row, 0 = complete
		)

		if err := rows.Scan(
			&event.SCN,
			&event.SQLRedo,
			&event.Operation,
			&event.TableName,
			&event.SchemaName,
			&event.Timestamp,
			&event.TransactionID,
			&commitSCN,
			&csf,
		); err != nil {
			return lastSCN, err
		}

		if haveLastProcessed && event.SCN > lastProcessedSCN {
			lastSCN = lastProcessedSCN
		}

		// CSF (Continuation SQL Flag): Oracle splits long SQL across multiple rows.
		// Rows with CSF=1 are continuation fragments; CSF=0 is the final (or only) row.
		// Concatenate all fragments before emitting the event.
		if pending != nil {
			// Append this fragment's SQL to the accumulated SQL.
			if event.SQLRedo.Valid {
				pending.SQLRedo.String += event.SQLRedo.String
			}
			if csf == 0 {
				// Final fragment — emit the accumulated event.
				if err := processEvent(ctx, pending); err != nil {
					return lastSCN, fmt.Errorf("processing redo event: %w", err)
				}
				lastProcessedSCN, haveLastProcessed = pending.SCN, true
				pending = nil
			}
			// If csf == 1, continue accumulating.
			continue
		}

		if csf == 1 {
			// Start accumulating a multi-part SQL.
			pending = event
			continue
		}

		if err := processEvent(ctx, event); err != nil {
			return lastSCN, fmt.Errorf("processing redo event: %w", err)
		}
		lastProcessedSCN, haveLastProcessed = event.SCN, true
	}

	if err := rows.Err(); err != nil {
		return lastSCN, err
	}

	// capture timings if 0 rows
	if firstRow {
		elapsed := time.Since(queryStart)
		lm.timeToFirstRowMetric.Timing(elapsed.Nanoseconds())
		lm.log.Debugf("LogMiner query returned no rows after %s (scn=%d to %d)", elapsed, startSCN, endSCN)
	}

	// Flush any incomplete pending event (shouldn't happen in practice).
	if pending != nil {
		lm.log.Warnf("Incomplete CSF SQL sequence at end of result set (scn=%d, op=%s, txn=%s)", pending.SCN, pending.Operation, pending.TransactionID)
		if err := processEvent(ctx, pending); err != nil {
			return lastSCN, fmt.Errorf("processing redo event: %w", err)
		}
		lastProcessedSCN, haveLastProcessed = pending.SCN, true
	}

	// The result set is exhausted — nothing more could share lastProcessedSCN, so it's now
	// confirmed complete too.
	if haveLastProcessed {
		lastSCN = lastProcessedSCN
	}

	return lastSCN, nil
}

// LogFile represents a redo or archive log file
type LogFile struct {
	FileName  string
	FirstSCN  uint64
	NextSCN   uint64
	Sequence  int64
	Type      string // "ONLINE" or "ARCHIVED"
	IsCurrent bool
	Thread    int
	Bytes     int64
}

// LogFileCollector finds relevant log files to mine
type LogFileCollector struct{}

// NewLogFileCollector creates a new *LogFileCollector which is responsible for
// discovering the relevant log files to mine.
func NewLogFileCollector() *LogFileCollector {
	return &LogFileCollector{}
}

// GetLogsBySCNRange collects log files whose SCN range overlaps [startSCN, endSCN].
func (*LogFileCollector) GetLogsBySCNRange(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64) ([]*LogFile, error) {
	query := `
		SELECT FILE_NAME, FIRST_CHANGE, NEXT_CHANGE, SEQ, TYPE, THREAD
		FROM (

			-- Online redo logs that overlap [startSCN, endSCN]
			SELECT
				MIN(F.MEMBER) AS FILE_NAME,
				L.FIRST_CHANGE# FIRST_CHANGE,
				L.NEXT_CHANGE# NEXT_CHANGE,
				L.SEQUENCE# AS SEQ,
				'ONLINE' AS TYPE,
				L.THREAD# AS THREAD
			FROM V$LOGFILE F, V$LOG L
			WHERE (L.STATUS = 'CURRENT' OR L.NEXT_CHANGE# >= :1)
			AND L.FIRST_CHANGE# <= :2
			AND F.GROUP# = L.GROUP#
			GROUP BY L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.SEQUENCE#, L.THREAD#

			UNION

			-- Archive logs that overlap [startSCN, endSCN]
			SELECT
				A.NAME AS FILE_NAME,
				A.FIRST_CHANGE# FIRST_CHANGE,
				A.NEXT_CHANGE# NEXT_CHANGE,
				A.SEQUENCE# AS SEQ,
				'ARCHIVED' AS TYPE,
				A.THREAD# AS THREAD
			FROM V$ARCHIVED_LOG A
			WHERE A.NAME IS NOT NULL
			AND A.ARCHIVED = 'YES'
			AND A.STATUS = 'A'
			AND A.NEXT_CHANGE# >= :1
			AND A.FIRST_CHANGE# <= :2
			AND A.DEST_ID IN (
				SELECT DEST_ID
				FROM V$ARCHIVE_DEST_STATUS
				WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1
			)
		)
		ORDER BY SEQ`

	rows, err := conn.QueryContext(ctx, query, startSCN, endSCN)
	if err != nil {
		return nil, fmt.Errorf("querying logs overlapping SCN range [%d, %d]: %w", startSCN, endSCN, err)
	}
	defer rows.Close()

	var archived, online []*LogFile
	for rows.Next() {
		lf := &LogFile{}
		if err := rows.Scan(&lf.FileName, &lf.FirstSCN, &lf.NextSCN, &lf.Sequence, &lf.Type, &lf.Thread); err != nil {
			return nil, fmt.Errorf("scanning logs row: %w", err)
		}
		lf.IsCurrent = lf.Type == "ONLINE"
		if lf.IsCurrent {
			online = append(online, lf)
		} else {
			archived = append(archived, lf)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deduplicateLogs(archived, online), nil
}

// GetMaxOnlineRedoLogBytes returns the size, in bytes, of the largest configured online redo
// log group. Used to translate a "number of logs" cap into an approximate byte-size threshold
// for the log_count windowing strategy, since archived logs can vary slightly in size (e.g.
// after a redo log resize) while online redo log groups are typically uniform.
func (*LogFileCollector) GetMaxOnlineRedoLogBytes(ctx context.Context, conn *sql.Conn) (int64, error) {
	var maxBytes int64
	if err := conn.QueryRowContext(ctx, "SELECT MAX(BYTES) FROM V$LOG").Scan(&maxBytes); err != nil {
		return 0, fmt.Errorf("querying maximum online redo log size: %w", err)
	}
	if maxBytes <= 0 {
		return 0, errors.New("database returned an invalid maximum online redo log size (V$LOG.BYTES)")
	}
	return maxBytes, nil
}

// GetLogsFromSCN collects all log files, online and archived, from startSCN forward with no
// upper bound, ordered by sequence ascending. Used by the log_count windowing strategy, which
// derives its own upper bound from however many of these logs it selects to mine (see
// capLogsByCount), rather than from a pre-computed SCN range.
func (*LogFileCollector) GetLogsFromSCN(ctx context.Context, conn *sql.Conn, startSCN uint64) ([]*LogFile, error) {
	query := `
		SELECT FILE_NAME, FIRST_CHANGE, NEXT_CHANGE, SEQ, TYPE, THREAD, BYTES, LOG_STATUS
		FROM (

			-- Online redo logs at or after startSCN. This can include inactive/active groups
			-- that have already switched out but aren't yet archived, alongside the log that's
			-- truly current — LOG_STATUS distinguishes them, since only the latter is safe to
			-- treat as having no fixed upper bound.
			SELECT
				MIN(F.MEMBER) AS FILE_NAME,
				L.FIRST_CHANGE# FIRST_CHANGE,
				L.NEXT_CHANGE# NEXT_CHANGE,
				L.SEQUENCE# AS SEQ,
				'ONLINE' AS TYPE,
				L.THREAD# AS THREAD,
				L.BYTES AS BYTES,
				L.STATUS AS LOG_STATUS
			FROM V$LOGFILE F, V$LOG L
			WHERE (L.STATUS = 'CURRENT' OR L.NEXT_CHANGE# >= :1)
			AND F.GROUP# = L.GROUP#
			GROUP BY L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.SEQUENCE#, L.THREAD#, L.BYTES, L.STATUS

			UNION

			-- Archive logs at or after startSCN
			SELECT
				A.NAME AS FILE_NAME,
				A.FIRST_CHANGE# FIRST_CHANGE,
				A.NEXT_CHANGE# NEXT_CHANGE,
				A.SEQUENCE# AS SEQ,
				'ARCHIVED' AS TYPE,
				A.THREAD# AS THREAD,
				A.BLOCKS * A.BLOCK_SIZE AS BYTES,
				'ARCHIVED' AS LOG_STATUS
			FROM V$ARCHIVED_LOG A
			WHERE A.NAME IS NOT NULL
			AND A.ARCHIVED = 'YES'
			AND A.STATUS = 'A'
			AND A.NEXT_CHANGE# >= :1
			AND A.DEST_ID IN (
				SELECT DEST_ID
				FROM V$ARCHIVE_DEST_STATUS
				WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1
			)
		)
		ORDER BY SEQ`

	rows, err := conn.QueryContext(ctx, query, startSCN)
	if err != nil {
		return nil, fmt.Errorf("querying logs from SCN %d: %w", startSCN, err)
	}
	defer rows.Close()

	var archived, online []*LogFile
	for rows.Next() {
		lf := &LogFile{}
		var logStatus string
		if err := rows.Scan(&lf.FileName, &lf.FirstSCN, &lf.NextSCN, &lf.Sequence, &lf.Type, &lf.Thread, &lf.Bytes, &logStatus); err != nil {
			return nil, fmt.Errorf("scanning logs row: %w", err)
		}
		lf.IsCurrent = logStatus == "CURRENT"
		if lf.Type == "ONLINE" {
			online = append(online, lf)
		} else {
			archived = append(archived, lf)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deduplicateLogs(archived, online), nil
}

// CheckLogStillCurrent reports whether the online redo log identified by (thread, sequence) is
// still the database's live current log. When it has switched out since being selected for mining,
// sealedNextSCN is its now-fixed NEXT_CHANGE# — the exact upper bound of what that log actually
// contains, safe to use in place of a since-read dbCurrentSCN that may already reflect a different,
// newer online log this mining cycle never loaded.
func (*LogFileCollector) CheckLogStillCurrent(ctx context.Context, conn *sql.Conn, log *LogFile) (stillCurrent bool, sealedNextSCN uint64, err error) {
	var status string
	if err := conn.QueryRowContext(ctx, `
		SELECT STATUS, NEXT_CHANGE# FROM V$LOG WHERE THREAD# = :1 AND SEQUENCE# = :2`,
		log.Thread, log.Sequence).Scan(&status, &sealedNextSCN); err != nil {
		return false, 0, fmt.Errorf("checking whether log thread=%d seq=%d is still current: %w", log.Thread, log.Sequence, err)
	}
	return status == "CURRENT", sealedNextSCN, nil
}

// capLogsByCount selects a prefix of logs (sorted ascending by sequence) whose cumulative size
// reaches count*maxLogBytes, mirroring Debezium 3.6's CappedLogFileSessionSelector. It returns
// allOnline=true when the selection reaches the end of logs without truncation and that last log
// is the current online redo log — meaning there's no backlog of completed logs to bound against,
// so the caller should mine without an upper cap instead.
func capLogsByCount(logs []*LogFile, count int, maxLogBytes int64) (capped []*LogFile, allOnline bool) {
	if len(logs) == 0 {
		return nil, false
	}
	if count <= 0 {
		// A non-positive count disables the cap entirely: mine every available log.
		return logs, logs[len(logs)-1].IsCurrent
	}

	threshold := int64(count) * maxLogBytes

	var accumulated int64
	for i, lf := range logs {
		accumulated += lf.Bytes
		if accumulated >= threshold || i == len(logs)-1 {
			capped = logs[:i+1]
			break
		}
	}

	return capped, capped[len(capped)-1].IsCurrent
}

// effectiveUpperBound computes the mining session's inclusive upper SCN boundary from a capped
// log selection. When the selection reaches the truly current log (allOnline), there's no fixed
// upper bound to respect: mine all the way to dbCurrentSCN. Otherwise the session is bounded by
// the capped set's own coverage.
//
// capped[last].NextSCN marks where that coverage ends, but that boundary value itself belongs to
// the *next* (unloaded) file, not this one: Oracle redo/archive logs use a half-open interval — a
// file's own content is [FirstSCN, NextSCN), and NextSCN is exactly the next file's FirstSCN.
// queryLogMinerContents treats its upper bound as inclusive (SCN <= endSCN), so capping to
// NextSCN itself would ask LogMiner for a row that only physically exists in a file that was
// never loaded — silently dropping any row whose SCN lands exactly on that boundary. Subtracting
// one keeps the bound within what's actually loaded. If that leaves no forward progress at all
// (an edge case: the capped set's own width is a single SCN unit), the caller's "unchanged, grow
// the count" logic naturally picks up more files on the next cycle rather than looping forever on
// a zero-width window.
func effectiveUpperBound(capped []*LogFile, allOnline bool, dbCurrentSCN uint64) uint64 {
	if allOnline {
		return dbCurrentSCN
	}
	if last := capped[len(capped)-1].NextSCN - 1; last < dbCurrentSCN {
		return last
	}
	return dbCurrentSCN
}

// sameLogFiles reports whether a and b select the identical set of logs (by thread+sequence),
// in the same order. Used to detect whether the mining session made forward progress between
// consecutive log_count cycles.
func sameLogFiles(a, b []*LogFile) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Thread != b[i].Thread || a[i].Sequence != b[i].Sequence {
			return false
		}
	}
	return true
}

// deduplicateLogs merges archive and online log lists, preferring the archive
// copy when the same (thread, sequence) exists in both (archived logs guarantee
// completeness where as online logs are still being written to). This prevents
// ORA-01289 when V$ARCHIVED_LOG contains multiple registrations of the same
// physical file, or when a sequence appears in both V$LOG and V$ARCHIVED_LOG.
func deduplicateLogs(archived, online []*LogFile) []*LogFile {
	type logKey struct {
		thread   int
		sequence int64
	}

	archivedKeys := make(map[logKey]struct{}, len(archived))
	for _, f := range archived {
		archivedKeys[logKey{f.Thread, f.Sequence}] = struct{}{}
	}

	out := make([]*LogFile, 0, len(archived)+len(online))
	out = append(out, archived...)
	for _, f := range online {
		if _, covered := archivedKeys[logKey{f.Thread, f.Sequence}]; !covered {
			out = append(out, f)
		}
	}
	return out
}

// startSessionWithLogFiles starts (or restarts) a LogMiner session over logFiles with explicit
// SCN bounds. Files are only reloaded (via ADD_LOGFILE) when the required set of logs changes -
// so the session is kept open across consecutive windows that cover the same log files.
func (lm *LogMiner) startSessionWithLogFiles(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64, logFiles []*LogFile) error {
	types := make([]string, len(logFiles))
	for i, f := range logFiles {
		types[i] = f.Type
	}
	lm.log.Debugf("Collected %d redo log file(s) for LogMiner: %v", len(logFiles), types)

	if lm.sessionMgr.logFilesChanged(logFiles) {
		// Log files have changed (first start or log switch) — full reload required.
		if lm.sessionMgr.IsActive() {
			if err := lm.sessionMgr.EndSession(ctx, conn); err != nil {
				lm.log.Errorf("Failed to end existing LogMiner session: %v", err)
			}
		}
		if err := lm.sessionMgr.AddLogFile(ctx, conn, logFiles); err != nil {
			return fmt.Errorf("loading %d log files into logminer: %w", len(logFiles), err)
		}
	}

	if err := lm.sessionMgr.StartSession(ctx, conn, startSCN, endSCN, false); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}

	lm.log.Debugf("Started LogMiner session from SCN %d to SCN %d", startSCN, endSCN)

	return nil
}

func toMessageEvent(dml *sqlredo.DMLEvent, scn uint64, checkpointSCN uint64, commitTimestamp time.Time) *replication.MessageEvent {
	var data map[string]any
	switch dml.Operation {
	case sqlredo.OpDelete:
		// column values are parsed into OldValues, not Data.
		data = dml.OldValues
	case sqlredo.OpUpdate:
		// merge new values onto old value for a current view that includes the PK
		data = make(map[string]any, len(dml.OldValues))
		maps.Copy(data, dml.OldValues)
		maps.Copy(data, dml.Data)
	default:
		data = dml.Data
	}

	m := &replication.MessageEvent{
		SCN:             replication.SCN(scn),
		CheckpointSCN:   replication.SCN(checkpointSCN),
		Schema:          dml.Schema,
		Table:           dml.Table,
		Data:            data,
		Timestamp:       dml.Timestamp,
		TransactionID:   dml.TransactionID.String(),
		CommitTimestamp: commitTimestamp,
	}

	switch dml.Operation {
	case sqlredo.OpInsert:
		m.Operation = replication.MessageOperationInsert
	case sqlredo.OpUpdate:
		m.Operation = replication.MessageOperationUpdate
	case sqlredo.OpDelete:
		m.Operation = replication.MessageOperationDelete
	}

	return m
}

func deferMiningCycle(currentSCN, dbCurrentSCN uint64, minWindowSize int) bool {
	// check to see if SCN window size is greater than configured value
	if minWindowSize <= 0 || dbCurrentSCN <= currentSCN {
		return false
	}
	return dbCurrentSCN-currentSCN < uint64(minWindowSize)
}

func adaptWindowSize(currentSize int, hitCap bool, minSize, maxSize, increment int) int {
	if hitCap {
		return min(currentSize+increment, maxSize)
	}
	return max(currentSize-increment, minSize)
}
