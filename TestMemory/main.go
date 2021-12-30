package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"test/utils"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

type oracleConfig struct {
	User        string
	Password    string
	Host        string
	Port        int64
	ServiceName string
}

func main() {
	utils.CoverString()
	meta := oracleConfig{
		User:        "roma_logminer",
		Password:    "oracle",
		Host:        "10.186.61.106",
		Port:        1521,
		ServiceName: "xe",
	}
	go StartWebService()
	oracleConn, err := openDB(meta)
	if err != nil {
		fmt.Printf("opendb fail : %v", err)
		return
	}
	l := &LogMinerStream{
		oracleDB:                 oracleConn,
		startScn:                 0,
		committedScn:             0,
		interval:                 100000,
		initialized:              false,
		currentRedoLogSequenceFP: "",
		OracleTxNum:              0,
	}

	for {
		time.Sleep(5 * time.Second)
		// changed, err := l.checkRedoLogChanged()
		// if err != nil {
		// 	return err
		// }
		// if changed {
		// 	err := l.stopLogMiner()
		// 	if err != nil {
		// 		return err
		// 	}
		// 	err = l.initLogMiner()
		// 	if err != nil {
		// 		return err
		// 	}
		// }
		err = l.initLogMiner()
		if err != nil {
			return
		}
		endScn, _ := l.GetCurrentSnapshotSCN()
		if endScn > l.startScn+l.interval {
			endScn = l.startScn + l.interval
		}
		// endScn, err := l.getEndScn()
		// if err != nil {
		// 	return err
		// }
		// if endScn == l.startScn {
		// 	continue
		// }

		err = l.StartLogMinerBySCN2(l.startScn, endScn)
		if err != nil {
			fmt.Println("StartLMBySCN ", err)
			return
		}
		// l.logger.Info("Get log miner record form", "StartScn", l.startScn, "EndScn", endScn)
		records := make(chan *LogMinerRecord, 100)
		err = l.GetLogMinerRecord(l.startScn, l.startScn+10000, records)
		if err != nil {
			// l.logger.Error("GetLogMinerRecord ", "err", err)
			return
		}
		l.startScn = endScn
	}
	// startLoopQuery(oracleConn)

}

func startLoopQuery(oracleConn *sql.Conn) {
	testList := make([]*TEST, 0)
	for {
		num := 0
		rows, err := oracleConn.QueryContext(context.TODO(), "SELECT ORDER_ID FROM SOE.ORDER_ITEMS")
		if err != nil {
			fmt.Printf("query fail : %v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			testCol := new(TEST)
			err := rows.Scan(&testCol.ADDRESS_ID)
			if err != nil {
				fmt.Printf("error on get connection:%v", err)
				return
			}
			testList = append(testList, testCol)
			num += 1
		}
		fmt.Println(num)
		time.Sleep(time.Second * 5)
	}
}

func openDB(meta oracleConfig) (*sql.Conn, error) {
	sqlDB, err := sql.Open("oracle", fmt.Sprintf("oracle://%s:%s@%s:%d/%s", meta.User, meta.Password, meta.Host, meta.Port, meta.ServiceName))
	if err != nil {
		return nil, fmt.Errorf("error on open oracle database connection:%v", err)
	}
	oracleConn, err := sqlDB.Conn(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error on get connection:%v", err)
	}
	return oracleConn, nil
}

func StartWebService() {
	http.HandleFunc("/", root)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("err:", err)
	}
}

func root(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello World!"))
}

type TEST struct {
	ADDRESS_ID int `json:"ADDRESS_ID" `
	ORDER_ID   int `json:"ORDER_ID" `
}

// ==========

type LogMinerStream struct {
	oracleDB                 *sql.Conn
	startScn                 int64
	committedScn             int64
	interval                 int64
	initialized              bool
	currentRedoLogSequenceFP string
	// logger                   g.LoggerType
	// txCache                  *LogMinerTxCache
	// replicateDB              []*common.DataSource
	// ignoreReplicateDB        []*common.DataSource
	OracleTxNum uint32
}

func (l *LogMinerStream) GetCurrentSnapshotSCN() (int64, error) {
	var globalSCN int64
	// 获取当前 SCN 号
	err := l.oracleDB.QueryRowContext(context.TODO(), "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&globalSCN)
	if err != nil {
		return 0, err
	}
	return globalSCN, nil
}

type LogFile struct {
	Name        string
	FirstChange int64
}

func (l *LogMinerStream) GetLogFileBySCN(scn int64) ([]*LogFile, error) {
	query := fmt.Sprintf(`
SELECT
    MIN(name) name,
    first_change#
FROM
    (
        SELECT
            MIN(member) AS name,
            first_change#,
            281474976710655 AS next_change#
        FROM
            v$log       l
            INNER JOIN v$logfile   f ON l.group# = f.group#
        WHERE l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE'
        GROUP BY
            first_change#
        UNION
        SELECT
            name,
            first_change#,
            next_change#
        FROM
            v$archived_log
        WHERE
            name IS NOT NULL
    )
WHERE
    first_change# >= %d
    OR %d < next_change#
GROUP BY
    first_change#
ORDER BY
    first_change#
`, scn, scn)

	rows, err := l.oracleDB.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fs []*LogFile
	for rows.Next() {
		f := &LogFile{}
		err = rows.Scan(&f.Name, &f.FirstChange)
		if err != nil {
			return nil, err
		}
		// l.logger.Debug("logFileName", "name", f.Name)
		fs = append(fs, f)
	}
	return fs, nil
}

func (l *LogMinerStream) AddLogMinerFile(fs []*LogFile) error {
	for i, f := range fs {
		var query string
		if i == 0 {
			query = fmt.Sprintf(`BEGIN
DBMS_LOGMNR.add_logfile ( '%s', DBMS_LOGMNR.new );
END;`, f.Name)
		} else {
			query = fmt.Sprintf(`BEGIN
DBMS_LOGMNR.add_logfile ( '%s' );
END;`, f.Name)
		}
		_, err := l.oracleDB.ExecContext(context.TODO(), query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *LogMinerStream) BuildLogMiner() error {
	query := `BEGIN 
DBMS_LOGMNR_D.build (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);
END;`
	_, err := l.oracleDB.ExecContext(context.TODO(), query)
	return err
}

func (l *LogMinerStream) StartLogMinerBySCN2(startScn, endScn int64) error {
	query := fmt.Sprintf(`
BEGIN
DBMS_LOGMNR.start_logmnr (
startSCN => %d,
endScn => %d,
options => SYS.DBMS_LOGMNR.skip_corruption +
SYS.DBMS_LOGMNR.no_sql_delimiter +
SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT +
SYS.DBMS_LOGMNR.DICT_FROM_REDO_LOGS +
SYS.DBMS_LOGMNR.DDL_DICT_TRACKING 
);
END;`, startScn, endScn)
	// l.logger.Debug("startLogMiner2", "query", query)
	_, err := l.oracleDB.ExecContext(context.TODO(), query)
	return err
}

func (l *LogMinerStream) StartLogMinerBySCN(scn int64) error {
	query := fmt.Sprintf(`
BEGIN
DBMS_LOGMNR.start_logmnr (
startSCN => %d,
options => SYS.DBMS_LOGMNR.skip_corruption +
SYS.DBMS_LOGMNR.no_sql_delimiter +
SYS.DBMS_LOGMNR.no_rowid_in_stmt +
SYS.DBMS_LOGMNR.dict_from_online_catalog +
SYS.DBMS_LOGMNR.string_literals_in_stmt 
);
END;`, scn)
	_, err := l.oracleDB.ExecContext(context.TODO(), query)
	return err
}

func (l *LogMinerStream) EndLogMiner() error {
	query := `
BEGIN
DBMS_LOGMNR.end_logmnr ();
END;`
	_, err := l.oracleDB.ExecContext(context.TODO(), query)
	return err
}

type LogMinerRecord struct {
	SCN       int64
	SegOwner  string
	TableName string
	SQLRedo   string
	SQLUndo   string
	Operation int
	XId       []byte
	Csf       int
	RowId     string
	Rollback  int
	RsId      string
	StartTime string
	Username  string
}

func (r *LogMinerRecord) TxId() string {
	h := md5.New()
	h.Write(r.XId)
	return hex.EncodeToString(h.Sum(nil))
}

func (r *LogMinerRecord) String() string {
	return fmt.Sprintf(`
scn: %d, seg_owner: %s, table_name: %s, op: %d, xid: %s, rb: %d
row_id: %s, username: %s,
sql: %s`, r.SCN, r.SegOwner, r.TableName, r.Operation, r.TxId(), r.Rollback,
		r.RowId, r.Username,
		r.SQLRedo,
	)
}

func (l *LogMinerStream) GetLogMinerRecord(startScn, endScn int64, records chan *LogMinerRecord) error {
	// AND table_name IN ('%s')
	// strings.Join(sourceTableNames, `','`),
	query := fmt.Sprintf(`
SELECT 
	scn,
	seg_owner,
	table_name,
	sql_redo,
	sql_undo,
	operation_code,
	xid,
	csf,
	row_id,
	rollback,
	rs_id,
	timestamp,
	username
FROM 
	V$LOGMNR_CONTENTS
WHERE
	SCN > %d
    AND SCN <= %d
	AND (
		(operation_code IN (6,7,34,36))
		OR 
		(operation_code IN (1,2,3,5) AND seg_owner not in ('SYS','SYSTEM','APPQOSSYS','AUDSYS','CTXSYS','DVSYS','DBSFWUSER',
		'DBSNMP','GSMADMIN_INTERNAL','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA',
    	'ORDSYS','OUTLN','WMSYS','XDB') %s
		)
	)
`, startScn, endScn, "AND ( seg_owner = 'SOE')")

	//l.logger.Debug("Get logMiner record", "QuerySql", query)
	rows, err := l.oracleDB.QueryContext(context.TODO(), query)
	if err != nil {
		return err
	}
	defer rows.Close()
	records1 := make([]*LogMinerRecord, 0)
	scan := func(rows *sql.Rows) (*LogMinerRecord, error) {
		lr := &LogMinerRecord{}
		var segOwner, tableName, undoSQL sql.NullString
		err = rows.Scan(&lr.SCN, &segOwner, &tableName, &lr.SQLRedo, &undoSQL, &lr.Operation, &lr.XId,
			&lr.Csf, &lr.RowId, &lr.Rollback, &lr.RsId, &lr.StartTime, &lr.Username)
		if err != nil {
			return nil, err
		}
		lr.SegOwner = segOwner.String
		lr.TableName = tableName.String
		lr.SQLUndo = undoSQL.String
		return lr, nil
	}
	recordsNum := 0
	// var lrs []*LogMinerRecord
	for rows.Next() {
		lr, err := scan(rows)
		if err != nil {
			return err
		}
		// 1 = indicates that either SQL_REDO or SQL_UNDO is greater than 4000 bytes in size
		// and is continued in the next row returned by the view
		if lr.Csf == 1 {
			redoLog := strings.Builder{}
			undoLog := strings.Builder{}
			redoLog.WriteString(lr.SQLRedo)
			undoLog.WriteString(lr.SQLUndo)
			for rows.Next() {
				lr2, err := scan(rows)
				if err != nil {
					return err
				}
				redoLog.WriteString(lr2.SQLRedo)
				undoLog.WriteString(lr2.SQLUndo)
				if lr2.Csf != 1 {
					break
				}
			}
			lr.SQLRedo = redoLog.String()
			lr.SQLUndo = undoLog.String()
		}
		recordsNum += 1
		// records <- lr
		records1 = append(records1, lr)
	}
	for _, r := range records1 {
		if r.Operation == 1 || r.Operation == 2 || r.Operation == 3 || r.Operation == 5 {
			fmt.Println(r.SQLRedo)
		}
	}
	// l.logger.Debug("Get logMiner record end", "recordsNum", recordsNum)
	return nil
}

func (l *LogMinerStream) initLogMiner() error {
	// l.logger.Debug("build logminer")
	err := l.BuildLogMiner()
	if err != nil {
		// l.logger.Error("BuildLogMiner ", "err", err)
		return err
	}

	fs, err := l.GetLogFileBySCN(l.startScn)
	if err != nil {
		// l.logger.Error("GetLogFileBySCN", "err", err)
		return err
	}

	// l.logger.Debug("add logminer file")
	err = l.AddLogMinerFile(fs)
	if err != nil {
		fmt.Println("AddLogMinerFile ", err)
		return err
	}
	// fp, err := l.oracleDB.CurrentRedoLogSequenceFp()
	// if err != nil {
	// 	// l.logger.Error("currentRedoLogSequenceFp ", "err", err)
	// 	return err
	// }

	// reset date/timestamp format
	SQL_ALTER_DATE_FORMAT := `ALTER SESSION SET NLS_DATE_FORMAT = 'SYYYY-MM-DD HH24:MI:SS'`
	_, err = l.oracleDB.QueryContext(context.TODO(), SQL_ALTER_DATE_FORMAT)
	if err != nil {
		return err
	}
	NLS_TIMESTAMP_FORMAT := "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'SYYYY-MM-DD HH24:MI:SS.FF6'"
	_, err = l.oracleDB.QueryContext(context.TODO(), NLS_TIMESTAMP_FORMAT)
	if err != nil {
		return err
	}

	// l.currentRedoLogSequenceFP = fp
	return nil
}
