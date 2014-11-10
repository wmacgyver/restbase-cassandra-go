package restbasesandra

import (
	"fmt"
	"testing"
)

func TestMakeClient(t *testing.T) {
	fmt.Println("MakeClient")
	var db DB
	err := db.MakeClient("", "localhost")
	defer db.Close()
	if err != nil {
		t.Fatal("connect:", err)
	}
}

func TestCreateTable(t *testing.T) {
	fmt.Println("CreateTable")
	var db DB
	err := db.MakeClient("", "localhost")
	defer db.Close()
	if err != nil {
		t.Fatal("connect:", err)
	}
	fmt.Println("simple table")
	req := []byte(`{
                "domain": "en.wikipedia.org",
                "table": "simpleTable",
                "options": { "storageClass": "SimpleStrategy", "durabilityLevel": 1 },
                "attributes": {
                    "key": "string",
                    "tid": "timeuuid",
                    "latestTid": "timeuuid",
                    "body": "blob",
                        "content-type": "string",
                        "content-length": "varint",
                            "content-sha256": "string",
                            "content-location": "string",
                    "restrictions": "set<string>"
                },
                "index": [
                    { "attribute": "key", "type": "hash" },
                    { "attribute": "latestTid", "type": "static" },
                    { "attribute": "tid", "type": "range", "order": "desc" }
                ]
            }`)
	err = db.CreateTable("org.wikipedia.en", req)
	if err != nil {
		t.Fatal("create table:", err)
	}

	fmt.Println("table with more than one range keys")
	req = []byte(`{
				"domain": "en.wikipedia.org",
				"table": "multiRangeTable",
				"options": {
					"storageClass": "SimpleStrategy",
					"durabilityLevel": 1
				},
				"attributes": {
					"key": "string",
					"tid": "timeuuid",
					"latestTid": "timeuuid",
					"uri": "string",
					"body": "blob",
					"restrictions": "set<string>"
				},
				"index": [{
					"attribute": "key",
					"type": "hash"
				}, {
					"attribute": "latestTid",
					"type": "static"
				}, {
					"attribute": "tid",
					"type": "range",
					"order": "desc"
				}, {
					"attribute": "uri",
					"type": "range",
					"order": "desc"
				}]
			}`)
	err = db.CreateTable("org.wikipedia.en", req)
	if err != nil {
		t.Fatal("create table:", err)
	}

	fmt.Println("table with secondary index")
	req = []byte(`{
				"domain": "en.wikipedia.org",
				"table": "simpleSecondaryIndexTable",
				"options": {
					"storageClass": "SimpleStrategy",
					"durabilityLevel": 1
				},
				"attributes": {
					"key": "string",
					"tid": "timeuuid",
					"latestTid": "timeuuid",
					"uri": "string",
					"body": "blob",
					"restrictions": "set<string>"
				},
				"index": [{
					"attribute": "key",
					"type": "hash"
				}, {
					"attribute": "tid",
					"type": "range",
					"order": "desc"
				}],
				"secondaryIndexes": {
					"by_uri": [{
						"attribute": "uri",
						"type": "hash"
					}, {
						"attribute": "body",
						"type": "proj"
					}]
				}
			}`)
	err = db.CreateTable("org.wikipedia.en", req)
	if err != nil {
		t.Fatal("create table:", err)
	}

	fmt.Println("table with secondary index and no tid in range")
	req = []byte(`{
				"domain": "en.wikipedia.org",
				"table": "unversionedSecondaryIndexTable",
				"options": {
					"storageClass": "SimpleStrategy",
					"durabilityLevel": 1
				},
				"attributes": {
					"key": "string",
					"latestTid": "timeuuid",
					"uri": "string",
					"body": "blob",
					"restrictions": "set<string>"
				},
				"index": [{
					"attribute": "key",
					"type": "hash"
				}, {
					"attribute": "uri",
					"type": "range",
					"order": "desc"
				}],
				"secondaryIndexes": {
					"by_uri": [{
						"attribute": "uri",
						"type": "hash"
					}, {
						"attribute": "key",
						"type": "range",
						"order": "desc"
					}, {
						"attribute": "body",
						"type": "proj"
					}]
				}
			}`)
	err = db.CreateTable("org.wikipedia.en", req)
	if err != nil {
		t.Fatal("create table:", err)
	}
}

func TestDropTable(t *testing.T) {
	fmt.Println("DropTable")
	var db DB
	err := db.MakeClient("", "localhost")
	defer db.Close()
	if err != nil {
		t.Fatal("connect:", err)
	}

	err = db.DropTable("org.wikipedia.en", "simpleTable")
	if err != nil {
		t.Fatal("dropTable:", err)
	}
	err = db.DropTable("org.wikipedia.en", "multiRangeTable")
	if err != nil {
		t.Fatal("dropTable:", err)
	}
	err = db.DropTable("org.wikipedia.en", "simpleSecondaryIndexTable")
	if err != nil {
		t.Fatal("dropTable:", err)
	}
	err = db.DropTable("org.wikipedia.en", "unversionedSecondaryIndexTable")
	if err != nil {
		t.Fatal("dropTable:", err)
	}
}
