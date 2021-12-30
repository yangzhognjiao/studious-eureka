// package main

// import (
// 	"context"
// 	"crypto/md5"
// 	"database/sql"
// 	"encoding/hex"
// 	"fmt"
// 	"strings"
// )

// type LogMinerStream struct {
// 	oracleDB                 *sql.Conn
// 	startScn                 int64
// 	committedScn             int64
// 	interval                 int64
// 	initialized              bool
// 	currentRedoLogSequenceFP string
// 	// logger                   g.LoggerType
// 	// txCache                  *LogMinerTxCache
// 	// replicateDB              []*common.DataSource
// 	// ignoreReplicateDB        []*common.DataSource
// 	OracleTxNum uint32
// }

// func (l *LogMinerStream) GetCurrentSnapshotSCN() (int64, error) {
// 	var globalSCN int64
// 	// 获取当前 SCN 号
// 	err := l.oracleDB.QueryRowContext(context.TODO(), "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&globalSCN)
// 	if err != nil {
// 		return 0, err
// 	}
// 	return globalSCN, nil
// }

// type LogFile struct {
// 	Name        string
// 	FirstChange int64
// }

// func (l *LogMinerStream) GetLogFileBySCN(scn int64) ([]*LogFile, error) {
// 	query := fmt.Sprintf(`
// SELECT
//     MIN(name) name,
//     first_change#
// FROM
//     (
//         SELECT
//             MIN(member) AS name,
//             first_change#,
//             281474976710655 AS next_change#
//         FROM
//             v$log       l
//             INNER JOIN v$logfile   f ON l.group# = f.group#
//         WHERE l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE'
//         GROUP BY
//             first_change#
//         UNION
//         SELECT
//             name,
//             first_change#,
//             next_change#
//         FROM
//             v$archived_log
//         WHERE
//             name IS NOT NULL
//     )
// WHERE
//     first_change# >= %d
//     OR %d < next_change#
// GROUP BY
//     first_change#
// ORDER BY
//     first_change#
// `, scn, scn)

// 	rows, err := l.oracleDB.QueryContext(context.TODO(), query)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	var fs []*LogFile
// 	for rows.Next() {
// 		f := &LogFile{}
// 		err = rows.Scan(&f.Name, &f.FirstChange)
// 		if err != nil {
// 			return nil, err
// 		}
// 		// l.logger.Debug("logFileName", "name", f.Name)
// 		fs = append(fs, f)
// 	}
// 	return fs, nil
// }

// func (l *LogMinerStream) AddLogMinerFile(fs []*LogFile) error {
// 	for i, f := range fs {
// 		var query string
// 		if i == 0 {
// 			query = fmt.Sprintf(`BEGIN
// DBMS_LOGMNR.add_logfile ( '%s', DBMS_LOGMNR.new );
// END;`, f.Name)
// 		} else {
// 			query = fmt.Sprintf(`BEGIN
// DBMS_LOGMNR.add_logfile ( '%s' );
// END;`, f.Name)
// 		}
// 		_, err := l.oracleDB.ExecContext(context.TODO(), query)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (l *LogMinerStream) BuildLogMiner() error {
// 	query := `BEGIN
// DBMS_LOGMNR_D.build (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);
// END;`
// 	_, err := l.oracleDB.ExecContext(context.TODO(), query)
// 	return err
// }

// func (l *LogMinerStream) StartLogMinerBySCN2(startScn, endScn int64) error {
// 	query := fmt.Sprintf(`
// BEGIN
// DBMS_LOGMNR.start_logmnr (
// startSCN => %d,
// endScn => %d,
// options => SYS.DBMS_LOGMNR.skip_corruption +
// SYS.DBMS_LOGMNR.no_sql_delimiter +
// SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT +
// SYS.DBMS_LOGMNR.DICT_FROM_REDO_LOGS +
// SYS.DBMS_LOGMNR.DDL_DICT_TRACKING
// );
// END;`, startScn, endScn)
// 	// l.logger.Debug("startLogMiner2", "query", query)
// 	_, err := l.oracleDB.ExecContext(context.TODO(), query)
// 	return err
// }

// func (l *LogMinerStream) StartLogMinerBySCN(scn int64) error {
// 	query := fmt.Sprintf(`
// BEGIN
// DBMS_LOGMNR.start_logmnr (
// startSCN => %d,
// options => SYS.DBMS_LOGMNR.skip_corruption +
// SYS.DBMS_LOGMNR.no_sql_delimiter +
// SYS.DBMS_LOGMNR.no_rowid_in_stmt +
// SYS.DBMS_LOGMNR.dict_from_online_catalog +
// SYS.DBMS_LOGMNR.string_literals_in_stmt
// );
// END;`, scn)
// 	_, err := l.oracleDB.ExecContext(context.TODO(), query)
// 	return err
// }

// func (l *LogMinerStream) EndLogMiner() error {
// 	query := `
// BEGIN
// DBMS_LOGMNR.end_logmnr ();
// END;`
// 	_, err := l.oracleDB.ExecContext(context.TODO(), query)
// 	return err
// }

// type LogMinerRecord struct {
// 	SCN       int64
// 	SegOwner  string
// 	TableName string
// 	SQLRedo   string
// 	SQLUndo   string
// 	Operation int
// 	XId       []byte
// 	Csf       int
// 	RowId     string
// 	Rollback  int
// 	RsId      string
// 	StartTime string
// 	Username  string
// }

// func (r *LogMinerRecord) TxId() string {
// 	h := md5.New()
// 	h.Write(r.XId)
// 	return hex.EncodeToString(h.Sum(nil))
// }

// func (r *LogMinerRecord) String() string {
// 	return fmt.Sprintf(`
// scn: %d, seg_owner: %s, table_name: %s, op: %d, xid: %s, rb: %d
// row_id: %s, username: %s,
// sql: %s`, r.SCN, r.SegOwner, r.TableName, r.Operation, r.TxId(), r.Rollback,
// 		r.RowId, r.Username,
// 		r.SQLRedo,
// 	)
// }

// func (l *LogMinerStream) GetLogMinerRecord(startScn, endScn int64, records chan *LogMinerRecord) error {
// 	// AND table_name IN ('%s')
// 	// strings.Join(sourceTableNames, `','`),
// 	query := fmt.Sprintf(`
// SELECT
// 	scn,
// 	seg_owner,
// 	table_name,
// 	sql_redo,
// 	sql_undo,
// 	operation_code,
// 	xid,
// 	csf,
// 	row_id,
// 	rollback,
// 	rs_id,
// 	timestamp,
// 	username
// FROM
// 	V$LOGMNR_CONTENTS
// WHERE
// 	SCN > %d
//     AND SCN <= %d
// 	AND (
// 		(operation_code IN (6,7,34,36))
// 		OR
// 		(operation_code IN (1,2,3,5) AND seg_owner not in ('SYS','SYSTEM','APPQOSSYS','AUDSYS','CTXSYS','DVSYS','DBSFWUSER',
// 		'DBSNMP','GSMADMIN_INTERNAL','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA',
//     	'ORDSYS','OUTLN','WMSYS','XDB') %s
// 		)
// 	)
// `, startScn, endScn, "AND ( seg_owner = 'SOE')")

// 	//l.logger.Debug("Get logMiner record", "QuerySql", query)
// 	rows, err := l.oracleDB.QueryContext(context.TODO(), query)
// 	if err != nil {
// 		return err
// 	}
// 	defer rows.Close()
// 	records1 := make([]*LogMinerRecord, 0)
// 	scan := func(rows *sql.Rows) (*LogMinerRecord, error) {
// 		lr := &LogMinerRecord{}
// 		var segOwner, tableName, undoSQL sql.NullString
// 		err = rows.Scan(&lr.SCN, &segOwner, &tableName, &lr.SQLRedo, &undoSQL, &lr.Operation, &lr.XId,
// 			&lr.Csf, &lr.RowId, &lr.Rollback, &lr.RsId, &lr.StartTime, &lr.Username)
// 		if err != nil {
// 			return nil, err
// 		}
// 		lr.SegOwner = segOwner.String
// 		lr.TableName = tableName.String
// 		lr.SQLUndo = undoSQL.String
// 		return lr, nil
// 	}
// 	recordsNum := 0
// 	// var lrs []*LogMinerRecord
// 	for rows.Next() {
// 		lr, err := scan(rows)
// 		if err != nil {
// 			return err
// 		}
// 		// 1 = indicates that either SQL_REDO or SQL_UNDO is greater than 4000 bytes in size
// 		// and is continued in the next row returned by the view
// 		if lr.Csf == 1 {
// 			redoLog := strings.Builder{}
// 			undoLog := strings.Builder{}
// 			redoLog.WriteString(lr.SQLRedo)
// 			undoLog.WriteString(lr.SQLUndo)
// 			for rows.Next() {
// 				lr2, err := scan(rows)
// 				if err != nil {
// 					return err
// 				}
// 				redoLog.WriteString(lr2.SQLRedo)
// 				undoLog.WriteString(lr2.SQLUndo)
// 				if lr2.Csf != 1 {
// 					break
// 				}
// 			}
// 			lr.SQLRedo = redoLog.String()
// 			lr.SQLUndo = undoLog.String()
// 		}
// 		recordsNum += 1
// 		// records <- lr
// 		records1 = append(records1, lr)
// 	}
// 	// l.logger.Debug("Get logMiner record end", "recordsNum", recordsNum)
// 	return nil
// }

// func (l *LogMinerStream) initLogMiner() error {
// 	// l.logger.Debug("build logminer")
// 	err := l.BuildLogMiner()
// 	if err != nil {
// 		// l.logger.Error("BuildLogMiner ", "err", err)
// 		return err
// 	}

// 	fs, err := l.GetLogFileBySCN(l.startScn)
// 	if err != nil {
// 		// l.logger.Error("GetLogFileBySCN", "err", err)
// 		return err
// 	}

// 	// l.logger.Debug("add logminer file")
// 	err = l.AddLogMinerFile(fs)
// 	if err != nil {
// 		fmt.Println("AddLogMinerFile ", err)
// 		return err
// 	}
// 	// fp, err := l.oracleDB.CurrentRedoLogSequenceFp()
// 	// if err != nil {
// 	// 	// l.logger.Error("currentRedoLogSequenceFp ", "err", err)
// 	// 	return err
// 	// }

// 	// err = l.oracleDB.NLS_DATE_FORMAT()
// 	// if err != nil {
// 	// 	l.logger.Error("alter date format ", "err", err)
// 	// 	return err
// 	// }

// 	// l.currentRedoLogSequenceFP = fp
// 	return nil
// }
