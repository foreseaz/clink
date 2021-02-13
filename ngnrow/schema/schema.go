package schema

import (
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Schema struct {
	Name     string
	Engine   string
	Query    string
	Tables   []Table
	TableMap map[string]*Table `yaml:"-"` // tableName:table
}

type Table struct {
	Name        string
	Topic       string
	OptTypePath string
	Pk          string
	Cols        []Col
	Index       []string
}

type Col struct {
	Name       string
	Type       string
	Extra      string
	InsertPath string
	UpdatePath string
}

func LoadConf(configPath string) (schema *Schema, err error) {
	var configBytes []byte
	if configBytes, err = ioutil.ReadFile(configPath); err != nil {
		log.WithError(err).Error("read config file failed")
	}
	schema = &Schema{}
	if err = yaml.Unmarshal(configBytes, schema); err != nil {
		log.WithError(err).Error("unmarshal config file failed")
		return nil, err
	}
	schema.TableMap = make(map[string]*Table)
	for _, t := range schema.Tables {
		schema.TableMap[t.Name] = &t
	}
	return
}

func (t *Table) DDL() (ddl []string) {
	/*
		`CREATE TABLE IF NOT EXISTS "indexed_blocks" (
			"height"		INTEGER PRIMARY KEY,
			"hash"			TEXT NOT NULL,
			"timestamp"		INTEGER DEFAULT 0,
			"version"		INTEGER,
			"producer"		TEXT,
			"merkle_root"	TEXT,
			"parent"		TEXT,
			"tx_count"		INTEGER
		);`,
	*/
	ddl = make([]string, 1)

	cols := ""
	for i, col := range t.Cols {
		if col.Name == t.Pk {
			cols += fmt.Sprintf(`'%s' %s PRIMARY KEY`, col.Name, col.Type)
		} else {
			cols += fmt.Sprintf(`'%s' %s`, col.Name, col.Type)
		}
		if col.Extra != "" {
			cols += fmt.Sprintf(" %s", col.Extra)
		}
		if i != len(t.Cols)-1 {
			cols += ",\n"
		}
	}
	ddl[0] = fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s' (\n%s\n);", t.Name, cols)

	for _, idx := range t.Index {
		ddl = append(ddl,
			fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS 'idx__%s__%s' ON `%s` (`%s`);",
				t.Name, idx, t.Name, idx,
			))
	}
	return
}
