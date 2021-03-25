package core

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/yaml.v3"
)

var conf = &Schema{
	Name:   "schema",
	Engine: "compact+:memory:",
	Query: `SELECT SUBSTRING(t1.TRANS_DATE, 0, 10) as trans_date,
t1.TRANS_BRAN_CODE as trans_bran_code,
ROUND(SUM(t1.TANS_AMT)/10000,2) as balance,
count(t1.rowid) as cnt
FROM mj t1
WHERE t1.MC_TRSCODE in ('INQ', 'LIS', 'CWD', 'CDP', 'TFR', 'PIN', 'REP', 'PAY')
AND t1.TRANS_FLAG = '0'
GROUP BY SUBSTRING(t1.TRANS_DATE, 0, 10),t1.TRANS_BRAN_CODE`,
	Tables: []Table{
		{
			Name: "mj",
			Type: "kafka",
			KafkaSrc: KafkaSrc{
				EndPointConfig: `{"bootstrap.servers": "192.168.10.90:9092,192.168.10.90:9093,192.168.10.90:9094", "broker.address.family": "v4",}`,
				Topic:          "mj",
				OptTypePath:    "optype",
			},
			Pk: "rowid",
			Cols: []Col{
				{
					Name:       "rowid",
					Type:       "string",
					Extra:      "NOT NULL",
					InsertPath: "rowid",
					UpdatePath: "rowid",
				},
				{
					Name:       "scntime",
					Type:       "bigint",
					Extra:      "",
					InsertPath: "scntime",
					UpdatePath: "scntime",
				},
				{
					Name:       "TANS_AMT",
					Type:       "bigint",
					Extra:      "DEFAULT 0",
					InsertPath: "after.TANS_AMT",
					UpdatePath: "after.TANS_AMT",
				},
				{
					Name:       "TRANS_FLAG",
					Type:       "string",
					Extra:      "",
					InsertPath: "after.TRANS_FLAG",
					UpdatePath: "after.TRANS_FLAG",
				},
				{
					Name:       "TRANS_DATE",
					Type:       "date",
					Extra:      "",
					InsertPath: "after.TRANS_DATE",
					UpdatePath: "after.TRANS_DATE",
				},
				{
					Name:       "TRANS_BRAN_CODE",
					Type:       "string",
					Extra:      "",
					InsertPath: "after.TRANS_BRAN_CODE",
					UpdatePath: "after.TRANS_BRAN_CODE",
				},
				{
					Name:       "MC_TRSCODE",
					Type:       "string",
					Extra:      "",
					InsertPath: "after.MC_TRSCODE",
					UpdatePath: "after.MC_TRSCODE",
				},
			},
			Index: []string{
				"TRANS_FLAG", "TRANS_DATE", "TRANS_BRAN_CODE",
			},
		},
	},
}

var confHybrid = &Schema{
	Name:   "schema",
	Engine: "compact+:memory:",
	Query: `SELECT SUBSTRING(t1.TRANS_DATE, 0, 10) as trans_date,
t1.TRANS_BRAN_CODE as trans_bran_code,
ROUND(SUM(t1.TANS_AMT)/10000,2) as balance,
count(t1.rowid) as cnt
FROM mj t1
WHERE t1.MC_TRSCODE in ('INQ', 'LIS', 'CWD', 'CDP', 'TFR', 'PIN', 'REP', 'PAY')
AND t1.TRANS_FLAG = '0'
GROUP BY SUBSTRING(t1.TRANS_DATE, 0, 10),t1.TRANS_BRAN_CODE`,
	Tables: []Table{
		{
			Name: "mj",
			Type: "kafka",
			KafkaSrc: KafkaSrc{
				EndPointConfig: `{"bootstrap.servers": "192.168.10.90:9092,192.168.10.90:9093,192.168.10.90:9094", "broker.address.family": "v4",}`,
				Topic:          "mj",
				OptTypePath:    "optype",
			},
			Pk: "rowid",
			Cols: []Col{
				{
					Name:       "rowid",
					Type:       "string",
					Extra:      "NOT NULL",
					InsertPath: "rowid",
					UpdatePath: "rowid",
				},
				{
					Name:       "scntime",
					Type:       "bigint",
					Extra:      "",
					InsertPath: "scntime",
					UpdatePath: "scntime",
				},
				{
					Name:       "TANS_AMT",
					Type:       "bigint",
					Extra:      "DEFAULT 0",
					InsertPath: "after.TANS_AMT",
					UpdatePath: "after.TANS_AMT",
				},
				{
					Name:       "TRANS_FLAG",
					Type:       "string",
					Extra:      "",
					InsertPath: "after.TRANS_FLAG",
					UpdatePath: "after.TRANS_FLAG",
				},
				{
					Name:       "TRANS_DATE",
					Type:       "date",
					Extra:      "",
					InsertPath: "after.TRANS_DATE",
					UpdatePath: "after.TRANS_DATE",
				},
				{
					Name:       "TRANS_BRAN_CODE",
					Type:       "string",
					Extra:      "",
					InsertPath: "after.TRANS_BRAN_CODE",
					UpdatePath: "after.TRANS_BRAN_CODE",
				},
				{
					Name:       "MC_TRSCODE",
					Type:       "string",
					Extra:      "",
					InsertPath: "after.MC_TRSCODE",
					UpdatePath: "after.MC_TRSCODE",
				},
			},
			Index: []string{
				"TRANS_FLAG", "TRANS_DATE", "TRANS_BRAN_CODE",
			},
		},
		{
			Name: "mysql_src1",
			Type: "mysql",
			MySQLSrc: MySQLSrc{
				Dsn:    "username:password@protocol(address)/dbname?param=value",
				Table:  "test_table",
				Select: "",
				TTL:    time.Hour,
			},
			Pk: "primary",
			Cols: []Col{
				{
					Name: "primary",
					Type: "bigint",
				},
				{
					Name: "idx_col",
					Type: "int",
				},
				{
					Name: "income",
					Type: "int",
				},
			},
			Index: []string{"idx_col"},
		},
		{
			Name: "mysql_src2",
			Type: "mysql",
			MySQLSrc: MySQLSrc{
				Dsn:    "username:password@protocol(address)/dbname2?param=value",
				Select: "SELECT primary2, idx_col2, income2 FROM mysql_src2 ORDER BY income2 DESC LIMIT 1",
				TTL:    2 * time.Hour,
			},
			Pk: "primary2",
			Cols: []Col{
				{
					Name: "primary2",
					Type: "bigint",
				},
				{
					Name: "idx_col2",
					Type: "int",
				},
				{
					Name: "income2",
					Type: "int",
				},
			},
			Index: []string{"idx_col2"},
		},
	},
}

func TestSchema(t *testing.T) {
	/*
		INSERT:
			{
			    "after": {
			        "TANS_AMT": "100.01",
			        "TRANS_FLAG": "P",
			        "TRANS_DATE": "2001-03-08 23:21:00",
			        "TRANS_BRAN_CODE": "11670103",
			        "MC_TRSCODE": "CWD"
			    },
			    "rowid": "623481",
			    "scntime": 984064860,
			    "optype": "INSERT",
			    "name": "MJ_JOUR"
			}
		UPDATE:
			{
				"rowid": "623481",
				"scntime": 984064861,
				"optype": "UPDATE",
				"name": "MJ_JOUR",
				"after": {
					"TRANS_FLAG": "0"
				},
				"before": {
					"TRANS_FLAG": "p"
				}
			}
	*/

	//Convey("Print schema config", t, func() {
	//	confByte, err := yaml.Marshal(conf)
	//	confByteFromFile, err2 := ioutil.ReadFile("../test/mj/schema_test.yaml")
	//	So(err, ShouldBeNil)
	//	So(err2, ShouldBeNil)
	//	So(string(confByte), ShouldResemble, string(confByteFromFile))
	//	So(
	//		// ignore the annoying auto indent by IDE
	//		string(bytes.ReplaceAll(confByte, []byte("  "), []byte(""))),
	//		ShouldResemble,
	//		string(bytes.ReplaceAll(confByteFromFile, []byte("  "), []byte(""))),
	//	)
	//})
	Convey("Load schema config", t, func() {
		schema, err := LoadConf("../test/mj/schema_test.yaml")
		So(err, ShouldBeNil)
		schemaStr, _ := yaml.Marshal(schema)
		confStr, _ := yaml.Marshal(conf)
		So(string(schemaStr), ShouldResemble, string(confStr))
	})
}
