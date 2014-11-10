package restbasesandra

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"strings"
)

type DB struct {
	session            *gocql.Session
	defaultConsistency gocql.Consistency
}

type Schema struct {
	Version          int                            `json:"version"`
	Table            string                         `json:"table"`
	Attributes       map[string]string              `json:"attributes"`
	Index            []map[string]string            `json:"index"`
	IKeys            []string                       `json:"iKeys"`
	IKeyMap          map[string]map[string]string   `json:"iKeyMap"`
	AttributeIndexes map[string]string              `json:"attributeIndexes"`
	SecondaryIndexes map[string][]map[string]string `json:"secondaryIndexes"`
	Versioned        bool                           `json:"versioned"`
	Tid              string                         `json:"tid"`
	Consistency      string                         `json:"consistency"`
}

var infoSchemaJson = []byte(` {
    "table": "meta",
    "attributes": {
        "key": "string",
        "value": "json"
    },
    "index": [
        { "attribute": "key", "type": "hash" }
    ],
    "iKeys": ["key"],
    "iKeyMap": {
        "key": { "attribute": "key", "type": "hash" }
    },
    "attributeIndexes": {}
	}`)

func (db *DB) MakeClient(keyspace string, hosts ...string) error {
	cluster := gocql.NewCluster(hosts...)
	if keyspace == "" {
		cluster.Keyspace = "system"
	} else {
		cluster.Keyspace = keyspace
	}
	session, err := cluster.CreateSession()
	db.session = session
	db.defaultConsistency = gocql.One
	return err
}

func (db *DB) Close() {
	db.session.Close()
}

func (db *DB) createTable(keyspace string, schema *Schema, tableName string, consistency gocql.Consistency) error {

	//TODO handle secondary indexes

	statics := make(map[string]bool)
	for _, v := range schema.Index {
		if v["type"] == "static" {
			statics[v["attribute"]] = true
		}
	}
	cql := "create table " + cassID(keyspace) + "." + cassID(tableName) + " ("

	for attr, colType := range schema.Attributes {
		cql += cassID(attr) + " "
		switch colType {
		case "blob":
			cql += "blob"
		case "set<blob>":
			cql += "set<blob>"
		case "decimal":
			cql += "decimal"
		case "set<decimal>":
			cql += "set<decimal>"
		case "double":
			cql += "double"
		case "set<double>":
			cql += "set<double>"
		case "boolean":
			cql += "boolean"
		case "set<boolean>":
			cql += "set<boolean>"
		case "int":
			cql += "varint"
		case "set<int>":
			cql += "set<varint>"
		case "varint":
			cql += "varint"
		case "set<varint>":
			cql += "set<varint>"
		case "string":
			cql += "text"
		case "set<string>":
			cql += "set<text>"
		case "timeuuid":
			cql += "timeuuid"
		case "set<timeuuid>":
			cql += "set<timeuuid>"
		case "uuid":
			cql += "uuid"
		case "set<uuid>":
			cql += "set<uuid>"
		case "timestamp":
			cql += "timestamp"
		case "set<timestamp>":
			cql += "set<timestamp>"
		case "json":
			cql += "text"
		case "set<json>":
			cql += "set<text>"
		}
		if statics[attr] {
			cql += " static"
		}
		cql += ", "
	}

	var hashBits []string
	var rangeBits []string
	var orderBits []string
	for _, index := range schema.Index {
		cassName := cassID(index["attribute"])
		if index["type"] == "hash" {
			hashBits = append(hashBits, cassName)
		} else if index["type"] == "range" {
			rangeBits = append(rangeBits, cassName)
			orderBits = append(orderBits, cassName+" "+index["order"])
		}
	}

	primaryKeys := []string{"(" + strings.Join(hashBits, ",") + ")"}
	primaryKeys = append(primaryKeys, rangeBits...)

	cql += "primary key ("
	cql += strings.Join(primaryKeys, ",") + "))"
	cql += " WITH compaction = { 'class' : 'LeveledCompactionStrategy' }"
	if len(orderBits) > 0 {
		cql += " and clustering order by ( " + strings.Join(orderBits, ",") + " )"
	}
	fmt.Println(cql)
	return db.session.Query(cql).Consistency(db.defaultConsistency).Exec()
}

func (db *DB) CreateTable(reverseDomain string, req []byte) error {
	var r Schema
	err := json.Unmarshal(req, &r)
	if err != nil {
		return err
	}
	if r.Table == "" {
		return errors.New("Table name required.")
	}
	keyspace := keyspaceName(reverseDomain, r.Table)
	consistency := db.defaultConsistency

	if r.Consistency != "" {
		switch r.Consistency {
		case "all":
			consistency = gocql.All
		case "localQuorum":
			consistency = gocql.LocalQuorum
		}
	}

	var infoSchema Schema
	err = json.Unmarshal(infoSchemaJson, &infoSchema)
	err = validateAndNormalizeSchema(&r)
	err = makeSchemaInfo(&r)

	//TODO figure out how to do the storage class in gocql

	err = db.createKeyspace(keyspace, consistency)
	if err != nil {
		return err
	}

	err = db.createTable(keyspace, &r, "data", consistency)
	if err != nil {
		return err
	}

	err = db.createTable(keyspace, &infoSchema, "meta", consistency)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) createKeyspace(keyspace string, consistency gocql.Consistency) error {
	cql := "create keyspace " + cassID(keyspace) + " WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1}"
	fmt.Println(cql)
	return db.session.Query(cql).Consistency(consistency).Exec()
}

func (db *DB) DropTable(reverseDomain string, table string) error {
	keyspace := keyspaceName(reverseDomain, table)
	return db.session.Query("drop keyspace " + cassID(keyspace)).Consistency(db.defaultConsistency).Exec()
}
