package restbasesandra

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"github.com/gocql/gocql"
	"io"
	"math"
	"regexp"
	"strings"
	"time"
)

func cassID(name string) string {
	matched, _ := regexp.MatchString("^[a-zA-Z0-9_]+$", name)
	if matched {
		return "\"" + name + "\""
	} else {
		return "\"" + strings.Replace(name, "\"", "\"\"", -1) + "\""
	}
}

func tidFromDate(date string) string {
	const longForm = "2006-01-02 15:04:05 -0700"
	dateTime, _ := time.Parse(longForm, date)
	uuid := gocql.UUIDFromTime(dateTime).String()[:18]
	node := "-9234-0123456789ab"
	return uuid + node
}

func hashKey(key string) string {
	hash := sha1.New()
	io.WriteString(hash, key)
	h := base64.StdEncoding.EncodeToString(hash.Sum(nil))
	re := regexp.MustCompile("[+/]") // Replace [+/] from base64 with _ (illegal in Cassandra)
	h = re.ReplaceAllString(h, "_")
	re = regexp.MustCompile("=+$") // Remove base64 padding, has no entropy
	h = re.ReplaceAllString(h, "")
	return h
}

func getValidPrefix(key string) string {
	re := regexp.MustCompile("^[a-zA-Z0-9_]+")
	prefixMatch := re.FindString(key)
	return prefixMatch
}

func makeValidKey(key string, length int) string {
	origKey := key
	key = strings.Replace(key, "_", "__", -1)
	key = strings.Replace(key, ".", "_", -1)
	matched, _ := regexp.MatchString("^[a-zA-Z0-9_]+$", key)
	if !matched {
		validPrefix := getValidPrefix(key)[:length*2/3]
		return validPrefix + hashKey(origKey)[:length-len(validPrefix)]
	} else if len(key) > length {
		return key[:length*2/3] + hashKey(origKey)[:length/3]
	} else {
		return key
	}
}

func keyspaceName(reverseDomain string, key string) string {
	prefix := makeValidKey(reverseDomain, int(math.Max(26.0, 48.0-float64(len(key)-3))))
	return prefix + "_T_" + makeValidKey(key, 48-len(prefix)-3)
}

func indexKeys(schema *Schema) error {
	var res []string
	for _, v := range schema.Index {
		if v["type"] == "hash" || v["type"] == "range" {
			res = append(res, v["attribute"])
		}
	}
	schema.IKeys = res
	return nil
}

func makeSchemaInfo(schema *Schema) error {
	schema.Versioned = false
	lastElem := schema.Index[len(schema.Index)-1]
	lastKey := lastElem["attribute"]

	schema.Attributes["_del"] = "timeuuid"
	if lastKey != "" && lastElem["type"] == "range" && lastElem["order"] == "desc" && schema.Attributes[lastKey] == "timeuuid" {
		schema.Tid = lastKey
	} else {
		schema.Attributes["_tid"] = "timeuuid"
		schema.Index = append(schema.Index, map[string]string{"attribute": "_tid", "type": "range", "order": "desc"})
		schema.Tid = "_tid"
	}

	indexKeys(schema)

	iKeyMap := make(map[string]map[string]string)

	for _, v := range schema.Index {
		iKeyMap[v["attribute"]] = v
	}
	schema.IKeyMap = iKeyMap

	// TODO add the secondary index stuff

	return nil
}

func validateIndexSchema(schema *Schema) error {
	if schema.Index == nil || len(schema.Index) == 0 {
		return errors.New("Invalid index ")
	}
	haveHash := false
	for _, v := range schema.Index {
		switch v["type"] {
		case "hash":
			haveHash = true
		case "range":
			if v["order"] != "asc" && v["order"] != "desc" {
				v["order"] = "desc"
			}
		case "static":
		case "proj":
		}
	}
	if !haveHash {
		return errors.New("Indexes without hash are not yet supported!")
	}
	return nil
}

func validateSecondaryIndexSchema(schema *Schema) error {
	if schema.SecondaryIndexes == nil || len(schema.SecondaryIndexes) == 0 {
		return errors.New("Invalid secondary index ")
	}
	haveHash := false
	for _, v := range schema.SecondaryIndexes {
		for _, attr := range v {
			switch attr["type"] {
			case "hash":
				haveHash = true
			case "range":
				if attr["order"] != "asc" && attr["order"] != "desc" {
					attr["order"] = "desc"
				}
			case "static":
			case "proj":
			}
		}
	}
	if !haveHash {
		return errors.New("Indexes without hash are not yet supported!")
	}
	return nil
}

func validateAndNormalizeSchema(schema *Schema) error {
	if schema.Version == 0 {
		schema.Version = 1
	} else if schema.Version != 1 {
		return errors.New("Schema version 1 expected")
	}
	err := validateIndexSchema(schema)
	if err != nil {
		return err
	}
	err = validateSecondaryIndexSchema(schema)
	if err != nil {
		return err
	}

	err = makeSchemaInfo(schema)

	return nil
}
