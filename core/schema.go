package core

import (
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
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
