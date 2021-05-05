package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/jackc/pgx/v4"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	// "github.com/jackc/pgx/v4"
)

func main() {
	port := flag.Int("p", 5432, "db port")
	user := flag.String("u", "", "db user")
	host := flag.String("h", "127.0.0.1", "db host")
	database := flag.String("d", "postgres", "db name")
	password := flag.String("password", "", "db password (use passwordFile instead when possible)")
	passwordFile := flag.String("passwordFile", "", "path to file containing password")
	exclude := flag.String("exclude", "", "databases and tables to exclude")

	path := flag.String("path", "/", "path to expose metrics")
	prefix := flag.String("prefix", "pg_", "stats prefix")
	listen := flag.String("listen", "127.0.0.1:9187", "listen address")
	minRows := flag.Int("minRows", 0, "ignores tables with fewer than specified number of rows")
	noGoStates := flag.Bool("noGoStats", false, "set to true to skip collecting built-in go stats")
	noProcessStats := flag.Bool("noProcessStats", false, "set to true to skip collecting built-in process stats")

	logLevel := flag.String("logLevel", "error", "the logging level (debug, info or error)")
	flag.Parse()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	switch strings.ToLower(*logLevel) {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "", "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		panic("unknown logLevel: " + *logLevel)
	}

	registry := prometheus.NewRegistry()

	if !*noGoStates {
		registry.MustRegister(prometheus.NewGoCollector())
	}
	if !*noProcessStats {
		registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	}

	var pass string
	if len(*password) != 0 {
		pass = strings.TrimSpace(*password)
	} else {
		passBytes, err := ioutil.ReadFile(*passwordFile)
		if err != nil {
			log.Fatal().Err(err).Str("path", *passwordFile).Msg("failed to open password file")
		}
		pass = strings.TrimSpace(string(passBytes))
	}

	uri := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", *user, pass, *host, *port, *database)
	connConfig, err := pgx.ParseConfig(uri)
	if err != nil {
		safeUri := fmt.Sprintf("postgres://%s:xxxxxx@%s:%d/%s", *user, *host, *port, *database)
		safeErr := strings.ReplaceAll(err.Error(), pass, "xxxxxx")
		log.Fatal().Str("error", safeErr).Str("uri", safeUri).Msg("invalid connection string")
	}

	exporter := NewExporter(ExporterOpts{
		prefix:     *prefix,
		minRows:    *minRows,
		connConfig: connConfig,
		exclude:    parseExclude(*exclude),
	})
	registry.MustRegister(exporter)

	http.Handle(*path, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	log.Info().Str("address", *listen).Msg("Server Starting")
	log.Fatal().Err(http.ListenAndServe(*listen, nil)).Send()
}

type Exclude struct {
	databases []string
	tables    map[string][]string
}

type ExporterOpts struct {
	prefix     string
	minRows    int
	connConfig *pgx.ConnConfig
	exclude    Exclude
}

type Exporter struct {
	sync.Mutex
	minRows    int
	exclude    Exclude
	connConfig *pgx.ConnConfig

	// global metrics
	glbBuffersClean        prometheus.Gauge
	glbBuffersBackend      prometheus.Gauge
	glbBuffersBackendFsync prometheus.Gauge
	glbMaxwrittenClean     prometheus.Gauge

	// database metrics
	dbSize            *prometheus.GaugeVec
	dbNumBackends     *prometheus.GaugeVec
	dbXactCommit      *prometheus.CounterVec
	dbXactRollback    *prometheus.CounterVec
	dbBlksRead        *prometheus.CounterVec
	dbBlksHit         *prometheus.CounterVec
	dbTupReturned     *prometheus.CounterVec
	dbTupFetched      *prometheus.CounterVec
	dbTupInserted     *prometheus.CounterVec
	dbTupUpdated      *prometheus.CounterVec
	dbTupDeleted      *prometheus.CounterVec
	dbConflicts       *prometheus.CounterVec
	dbTempFiles       *prometheus.CounterVec
	dbTempBytes       *prometheus.CounterVec
	dbDeadlocks       *prometheus.CounterVec
	dbConflTablespace *prometheus.CounterVec
	dbConflLock       *prometheus.CounterVec
	dbConflSnapshot   *prometheus.CounterVec
	dbConflBufferpin  *prometheus.CounterVec
	dbConflDeadlock   *prometheus.CounterVec

	//table metrics
	tblVacuumCount      *prometheus.CounterVec
	tblAutoVacuumCount  *prometheus.CounterVec
	tblAnalyzeCount     *prometheus.CounterVec
	tblAutoAnalyzeCount *prometheus.CounterVec
	tblSeqScan          *prometheus.CounterVec
	tblSeqTupRead       *prometheus.CounterVec
	tblIdxScan          *prometheus.CounterVec
	tblIdxTupFetch      *prometheus.CounterVec
	tblNTupIns          *prometheus.CounterVec
	tblNTupUpd          *prometheus.CounterVec
	tblNTupDel          *prometheus.CounterVec
	tblNTupHotUpd       *prometheus.CounterVec
	tblNLiveTup         *prometheus.GaugeVec
	tblNDeadTup         *prometheus.GaugeVec
	tblNModSinceAnalyze *prometheus.GaugeVec

	//slot metrics
	slotLag *prometheus.GaugeVec
}

func NewExporter(opts ExporterOpts) *Exporter {
	slotLabels := []string{"slot"}
	databaseLabels := []string{"database"}
	databaseAndTableLabels := []string{"database", "table"}

	return &Exporter{
		minRows:    opts.minRows,
		exclude:    opts.exclude,
		connConfig: opts.connConfig,

		// global metrics
		glbBuffersClean:        prometheus.NewGauge(prometheus.GaugeOpts{Name: opts.prefix + "bgw_buffers_clean"}),
		glbBuffersBackend:      prometheus.NewGauge(prometheus.GaugeOpts{Name: opts.prefix + "bgw_buffers_backend"}),
		glbBuffersBackendFsync: prometheus.NewGauge(prometheus.GaugeOpts{Name: opts.prefix + "bgw_buffers_backend_fsync"}),
		glbMaxwrittenClean:     prometheus.NewGauge(prometheus.GaugeOpts{Name: opts.prefix + "bgw_maxwritten_clean"}),

		// DB metrics
		dbSize:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: opts.prefix + "db_size"}, databaseLabels),
		dbNumBackends:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: opts.prefix + "db_numbackends"}, databaseLabels),
		dbXactCommit:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_xact_commit"}, databaseLabels),
		dbXactRollback:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_xact_rollback"}, databaseLabels),
		dbBlksRead:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_blks_read"}, databaseLabels),
		dbBlksHit:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_blks_hit"}, databaseLabels),
		dbTupReturned:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_tup_returned"}, databaseLabels),
		dbTupFetched:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_tup_fetched"}, databaseLabels),
		dbTupInserted:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_tup_inserted"}, databaseLabels),
		dbTupUpdated:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_tup_updated"}, databaseLabels),
		dbTupDeleted:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_tup_deleted"}, databaseLabels),
		dbConflicts:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_conflicts"}, databaseLabels),
		dbTempFiles:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_temp_files"}, databaseLabels),
		dbTempBytes:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_temp_bytes"}, databaseLabels),
		dbDeadlocks:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "db_deadlocks"}, databaseLabels),
		dbConflTablespace: prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "confl_tablespace"}, databaseLabels),
		dbConflLock:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "confl_lock"}, databaseLabels),
		dbConflSnapshot:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "confl_snapshot"}, databaseLabels),
		dbConflBufferpin:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "confl_bufferpin"}, databaseLabels),
		dbConflDeadlock:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "confl_deadlock"}, databaseLabels),

		//table metrics
		tblVacuumCount:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_vacuum_count"}, databaseAndTableLabels),
		tblAutoVacuumCount:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_autovacuum_count"}, databaseAndTableLabels),
		tblAnalyzeCount:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_analyze_count"}, databaseAndTableLabels),
		tblAutoAnalyzeCount: prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_autoanalyze_count"}, databaseAndTableLabels),
		tblSeqScan:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_seq_scan"}, databaseAndTableLabels),
		tblSeqTupRead:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_seq_tup_read"}, databaseAndTableLabels),
		tblIdxScan:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_idx_scan"}, databaseAndTableLabels),
		tblIdxTupFetch:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_idx_tup_read"}, databaseAndTableLabels),
		tblNTupIns:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_n_tup_ins"}, databaseAndTableLabels),
		tblNTupUpd:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_n_tup_upd"}, databaseAndTableLabels),
		tblNTupDel:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_n_tup_del"}, databaseAndTableLabels),
		tblNTupHotUpd:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: opts.prefix + "tbl_n_tup_hot_upd"}, databaseAndTableLabels),
		tblNLiveTup:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: opts.prefix + "tbl_n_live_tup"}, databaseAndTableLabels),
		tblNDeadTup:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: opts.prefix + "tbl_n_dead_tup"}, databaseAndTableLabels),
		tblNModSinceAnalyze: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: opts.prefix + "tbl_n_mod_since_analyze"}, databaseAndTableLabels),

		// slot metrics
		slotLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: opts.prefix + "slot_pg_wal_lsn_diff"}, slotLabels),
	}
}

func (e *Exporter) Describe(c chan<- *prometheus.Desc) {
}

func (e *Exporter) Collect(c chan<- prometheus.Metric) {
	e.Lock()
	defer e.Unlock()

	conn := e.connect("postgres")
	if conn == nil {
		return
	}
	defer conn.Close(context.Background())
	e.collectGlobal(c, conn)
	e.collectSlots(c, conn)
	databases := e.collectDatabases(c, conn)
	conn.Close(context.Background())

	for _, database := range databases {
		e.collectTables(c, database)
	}

}

func (e *Exporter) connect(database string) *pgx.Conn {
	config := e.connConfig
	config.Database = database

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Error().Err(err).Msg("failed to open connection")
		return nil
	}
	return conn
}

func (e *Exporter) collectGlobal(c chan<- prometheus.Metric, conn *pgx.Conn) {
	sql := `
		select
			buffers_clean,
			buffers_backend,
			buffers_backend_fsync,
			maxwritten_clean
		from pg_stat_bgwriter`

	var buffersClean, buffersBackend, buffersBackendFsync, maxWrittenClean int
	row := conn.QueryRow(context.Background(), sql)
	err := row.Scan(&buffersClean, &buffersBackend, &buffersBackendFsync, &maxWrittenClean)
	if err != nil {
		log.Error().Err(err).Msg("collect global")
		return
	}

	e.glbBuffersClean.Set(float64(buffersClean))
	e.glbBuffersBackend.Set(float64(buffersBackend))
	e.glbBuffersBackendFsync.Set(float64(buffersBackendFsync))
	e.glbMaxwrittenClean.Set(float64(maxWrittenClean))

	c <- e.glbBuffersClean
	c <- e.glbBuffersBackend
	c <- e.glbBuffersBackendFsync
	c <- e.glbMaxwrittenClean
}

func (e *Exporter) collectSlots(c chan<- prometheus.Metric, conn *pgx.Conn) {
	sql := `
		select
			slot_name,
			pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
		from pg_replication_slots`

	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		log.Error().Err(err).Msg("collectSlots")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var lag int
		var slot string

		err := rows.Scan(&slot, &lag)
		if err != nil {
			log.Error().Err(err).Msg("collectSlots scan")
			return
		}
		gauge(c, e.slotLag, float64(lag), slot)
	}

	return
}

func (e *Exporter) collectDatabases(c chan<- prometheus.Metric, conn *pgx.Conn) []string {
	sql := `
		select
			d.datname,
			pg_database_size(d.datname),
			s.numbackends,
			s.xact_commit,
			s.xact_rollback,
			s.blks_read,
			s.blks_hit,
			s.tup_returned,
			s.tup_fetched,
			s.tup_inserted,
			s.tup_updated,
			s.tup_deleted,
			s.conflicts,
			s.temp_files,
			s.temp_bytes,
			s.deadlocks,
			c.confl_tablespace,
			c.confl_lock,
			c.confl_snapshot,
			c.confl_bufferpin,
			c.confl_deadlock
		from pg_database as d
			join pg_stat_database as s using (datname)
			join pg_stat_database_conflicts as c using (datname)
		where d.datname is not null
			and d.datallowconn
			and ($1::text[] is null or d.datname != all($1))`

	rows, err := conn.Query(context.Background(), sql, e.exclude.databases)
	if err != nil {
		log.Error().Err(err).Msg("collectDatabase")
		return nil
	}
	defer rows.Close()

	databases := make([]string, 0, 10)

	for rows.Next() {
		var datName string
		var size, numBackends, xactCommit, xactRollback, blksRead, blksHit,
			tupReturned, tupFetched, tupInserted, tupUpdated, tupDeleted, conflicts,
			tempFiles, tempBytes, deadlocks,
			conflTablespace, conflLock, conflSnapshot, conflBufferpin, conflDeadlock int

		err = rows.Scan(
			&datName, &size, &numBackends, &xactCommit, &xactRollback,
			&blksRead, &blksHit, &tupReturned, &tupFetched, &tupInserted,
			&tupUpdated, &tupDeleted, &conflicts, &tempFiles, &tempBytes, &deadlocks,
			&conflTablespace, &conflLock, &conflSnapshot, &conflBufferpin, &conflDeadlock)

		if err != nil {
			log.Error().Err(err).Msg("collectDatabase scan")
			return nil
		}

		databases = append(databases, datName)

		gauge(c, e.dbSize, float64(size), datName)
		gauge(c, e.dbNumBackends, float64(numBackends), datName)
		counter(c, e.dbXactCommit, float64(xactCommit), datName)
		counter(c, e.dbXactRollback, float64(xactRollback), datName)
		counter(c, e.dbBlksRead, float64(blksRead), datName)
		counter(c, e.dbBlksHit, float64(blksHit), datName)
		counter(c, e.dbTupReturned, float64(tupReturned), datName)
		counter(c, e.dbTupFetched, float64(tupFetched), datName)
		counter(c, e.dbTupInserted, float64(tupInserted), datName)
		counter(c, e.dbTupUpdated, float64(tupUpdated), datName)
		counter(c, e.dbTupDeleted, float64(tupDeleted), datName)
		counter(c, e.dbConflicts, float64(conflicts), datName)
		counter(c, e.dbTempFiles, float64(tempFiles), datName)
		counter(c, e.dbTempBytes, float64(tempBytes), datName)
		counter(c, e.dbDeadlocks, float64(deadlocks), datName)
		counter(c, e.dbConflTablespace, float64(conflTablespace), datName)
		counter(c, e.dbConflLock, float64(conflLock), datName)
		counter(c, e.dbConflSnapshot, float64(conflSnapshot), datName)
		counter(c, e.dbConflBufferpin, float64(conflBufferpin), datName)
		counter(c, e.dbConflDeadlock, float64(conflDeadlock), datName)
	}

	return databases
}

func (e *Exporter) collectTables(c chan<- prometheus.Metric, database string) {
	conn := e.connect(database)
	if conn == nil {
		return
	}
	defer conn.Close(context.Background())

	sql := `
		select
			relname,
			coalesce(vacuum_count, 0),
			coalesce(autovacuum_count, 0),
			coalesce(analyze_count, 0),
			coalesce(autoanalyze_count, 0),
			coalesce(seq_scan, 0),
			coalesce(seq_tup_read, 0),
			coalesce(idx_scan, 0),
			coalesce(idx_tup_fetch, 0),
			coalesce(n_tup_ins, 0),
			coalesce(n_tup_upd, 0),
			coalesce(n_tup_del, 0),
			coalesce(n_tup_hot_upd, 0),
			coalesce(n_live_tup, 0),
			coalesce(n_dead_tup, 0),
			n_mod_since_analyze
		from pg_stat_user_tables
		where coalesce(n_live_tup, 0) >= $1
			and ($2::text[] is null or relname != all($2))
		`

	rows, err := conn.Query(context.Background(), sql, e.minRows, e.exclude.tables[database])
	if err != nil {
		log.Error().Err(err).Str("database", database).Msg("collectTables")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var relname string
		var vacuumCount, autoVacuumCount, analyzeCount, autoAnalyzeCount,
			seqScan, seqTupRead, idxScan, idxTupFetch, nTupIns, nTupUpd,
			nTupDel, nTupHotUpd, nLiveTup, nDeadTup, nModSinceAnalyze int

		err = rows.Scan(
			&relname, &vacuumCount, &autoVacuumCount, &analyzeCount, &autoAnalyzeCount,
			&seqScan, &seqTupRead, &idxScan, &idxTupFetch, &nTupIns, &nTupUpd,
			&nTupDel, &nTupHotUpd, &nLiveTup, &nDeadTup, &nModSinceAnalyze)

		if err != nil {
			log.Error().Err(err).Str("database", database).Msg("collectTable scan")
			return
		}

		counter(c, e.tblVacuumCount, float64(vacuumCount), database, relname)
		counter(c, e.tblAutoVacuumCount, float64(autoVacuumCount), database, relname)
		counter(c, e.tblAnalyzeCount, float64(analyzeCount), database, relname)
		counter(c, e.tblAutoAnalyzeCount, float64(autoAnalyzeCount), database, relname)
		counter(c, e.tblSeqScan, float64(seqScan), database, relname)
		counter(c, e.tblSeqTupRead, float64(seqTupRead), database, relname)
		counter(c, e.tblIdxScan, float64(idxScan), database, relname)
		counter(c, e.tblIdxTupFetch, float64(idxTupFetch), database, relname)
		counter(c, e.tblNTupIns, float64(nTupIns), database, relname)
		counter(c, e.tblNTupUpd, float64(nTupUpd), database, relname)
		counter(c, e.tblNTupDel, float64(nTupDel), database, relname)
		counter(c, e.tblNTupHotUpd, float64(nTupHotUpd), database, relname)
		gauge(c, e.tblNLiveTup, float64(nLiveTup), database, relname)
		gauge(c, e.tblNDeadTup, float64(nDeadTup), database, relname)
		gauge(c, e.tblNModSinceAnalyze, float64(nModSinceAnalyze), database, relname)
	}
}

func gauge(c chan<- prometheus.Metric, g *prometheus.GaugeVec, value float64, labels ...string) {
	gauge, err := g.GetMetricWithLabelValues(labels...)
	if err != nil {
		log.Error().Err(err).Msg("failed to create gauge")
	} else {
		gauge.Set(value)
		c <- gauge
	}
}

func counter(c chan<- prometheus.Metric, cnt *prometheus.CounterVec, value float64, labels ...string) {
	counter, err := cnt.GetMetricWithLabelValues(labels...)
	if err != nil {
		log.Error().Err(err).Msg("failed to create counter")
	} else {
		counter.Add(value)
		c <- counter
	}
}

func parseExclude(exclude string) Exclude {
	exclude = strings.TrimSpace(exclude)
	if len(exclude) == 0 {
		return Exclude{}
	}

	databases := make([]string, 0, 2)
	tables := make(map[string][]string)
	parts := strings.Split(exclude, ";")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		entry := strings.Split(part, ":")
		if len(entry) == 1 {
			database := entry[0]
			databases = append(databases, database)
			log.Info().Str("database", database).Msg("excluding database")
		} else if len(entry) == 2 {
			database := entry[0]
			for _, table := range strings.Split(strings.TrimSpace(entry[1]), ",") {
				if t, ok := tables[database]; ok {
					tables[database] = append(t, table)
				} else {
					tables[database] = []string{table}
				}
				log.Info().Str("database", database).Str("table", table).Msg("excluding table")
			}
		}
	}

	return Exclude{
		tables:    tables,
		databases: databases,
	}
}
