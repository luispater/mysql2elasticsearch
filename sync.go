package mysql2elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	mycli "github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"io"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

func SetLogger(w io.Writer) {
	sh, err := log.NewStreamHandler(w)
	if err != nil {
		panic(err)
	}
	l := log.NewDefault(sh)
	l.SetLevel(log.LevelInfo)
	log.SetDefaultLogger(l)
}

func NewSyncMySQLToElasticSearch(addr, user, password, dbName, esAddr, esUser, esPassword string, option ...SyncMySQLToElasticSearchOption) (*SyncMySQLToElasticSearch, error) {
	var err error
	sync := new(SyncMySQLToElasticSearch)
	if len(option) > 0 {
		err = sync.Init(addr, user, password, dbName, esAddr, esUser, esPassword, option[0])
	} else {
		err = sync.Init(addr, user, password, dbName, esAddr, esUser, esPassword, SyncMySQLToElasticSearchOption{})
	}

	if err != nil {
		return nil, err
	}
	return sync, nil
}

type SyncMySQLToElasticSearchOption struct {
	IndexNameSeparator string
	QueueSize          int
	MaxQueueSize       int
}

type SyncMySQLToElasticSearch struct {
	canal.DummyEventHandler
	tableFields            map[string][]string
	structMapping          map[string]map[string]DBDataTypeMapping
	structPrimaryKey       map[string]DBDataTypeMapping
	tableNameStructMapping map[string]interface{}
	esClient               *elastic.Client
	ctx                    context.Context
	conn                   *mycli.Conn
	addr                   string
	user                   string
	password               string
	dbName                 string
	esQueue                *Queue
	runSyncer              bool
	option                 SyncMySQLToElasticSearchOption
	onPosSynced            func(name string, pos uint32, force bool)
}

func (this *SyncMySQLToElasticSearch) SetOnPosSynced(onPostSynced func(name string, pos uint32, force bool)) {
	this.onPosSynced = onPostSynced
}

func (this *SyncMySQLToElasticSearch) syncer() {
	for this.runSyncer {
		intCount := 0
		bulkRequest := this.esClient.Bulk()
		for !this.esQueue.Empty() {
			intCount++

			infData, err := this.esQueue.Dequeue()
			if err != nil {
				log.Error(err)
				continue
			}
			if infData != nil {
				data := infData.(ESQueueItem)

				if data.Action == "insert" || data.Action == "update" {
					bulkRequest.Add(elastic.NewBulkIndexRequest().Index(data.IndexName).Id(data.PrimaryKey).Doc(this.getObjectValues(data.Data)))
				} else if data.Action == "delete" {
					bulkRequest.Add(elastic.NewBulkDeleteRequest().Index(data.IndexName).Id(data.PrimaryKey))
				}
				if intCount >= this.option.QueueSize {
					break
				}
			}
		}
		if bulkRequest.NumberOfActions() > 0 {
			resp, err := bulkRequest.Refresh("wait_for").Do(this.ctx)
			if err != nil {
				log.Error(err)
			}
			if len(resp.Failed()) > 0 {
				failed := resp.Failed()
				for failedIndex := range failed {
					if failed[failedIndex].Error != nil {
						log.Error(failed[failedIndex].Error.Reason)
					}
				}
			}
			log.Infof("%d documents synced", len(resp.Succeeded()))
			debug.FreeOSMemory()
		} else {

		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (this *SyncMySQLToElasticSearch) Init(addr, user, password, dbName, esAddr, esUser, esPassword string, option SyncMySQLToElasticSearchOption) error {
	var err error

	this.tableFields = make(map[string][]string)
	this.structMapping = make(map[string]map[string]DBDataTypeMapping)
	this.structPrimaryKey = make(map[string]DBDataTypeMapping)
	this.tableNameStructMapping = make(map[string]interface{})

	this.addr = addr
	this.user = user
	this.password = password
	this.dbName = dbName

	this.esQueue = NewQueue()

	if option.QueueSize == 0 {
		option.QueueSize = 10000
	}

	if option.MaxQueueSize == 0 {
		option.MaxQueueSize = 10000 * 3
	}

	if option.IndexNameSeparator == "" {
		option.IndexNameSeparator = "@"
	}

	this.option = option

	this.conn, err = mycli.Connect(addr, user, password, dbName)
	if err != nil {
		return err
	}
	err = this.conn.Ping()
	if err != nil {
		return err
	}

	servers := []string{esAddr}
	this.esClient, err = elastic.NewClient(
		elastic.SetURL(servers...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetBasicAuth(esUser, esPassword),
	)
	if err != nil {
		return err
	}

	this.ctx = context.Background()

	this.runSyncer = true
	go this.syncer()

	return nil
}

func (this *SyncMySQLToElasticSearch) addToESQueue(action, indexName, primaryKey string, data interface{}) {
	for this.esQueue.Size() > this.option.MaxQueueSize && this.option.MaxQueueSize != -1 {
		time.Sleep(50 * time.Millisecond)
	}
	this.esQueue.Enqueue(ESQueueItem{
		Action:     action,
		IndexName:  indexName,
		PrimaryKey: primaryKey,
		Data:       data,
	})
}

func (this *SyncMySQLToElasticSearch) parseTagSetting(tags reflect.StructTag) map[string]string {
	setting := map[string]string{}
	for _, str := range []string{tags.Get("sql"), tags.Get("gorm")} {
		if str == "" {
			continue
		}
		arrayTags := strings.Split(str, ";")
		for _, value := range arrayTags {
			v := strings.Split(value, ":")
			k := strings.TrimSpace(strings.ToUpper(v[0]))
			if len(v) >= 2 {
				setting[k] = strings.Join(v[1:], ":")
			} else {
				setting[k] = k
			}
		}
	}
	return setting
}

func (this *SyncMySQLToElasticSearch) RegisterTable(tableName string, tableStruct interface{}) error {
	r, err := this.conn.Execute(fmt.Sprintf("SELECT * FROM `%s` WHERE 1=0", tableName))
	if err != nil {
		return err
	}
	defer r.Close()

	this.tableFields[tableName] = make([]string, 0)
	this.structMapping[tableName] = make(map[string]DBDataTypeMapping)

	for i := 0; i < len(r.Fields); i++ {
		this.tableFields[tableName] = append(this.tableFields[tableName], string(r.Fields[i].Name))
	}

	var t reflect.Type
	rv := reflect.ValueOf(tableStruct)
	if rv.Kind() != reflect.Ptr {
		t = reflect.TypeOf(tableStruct)
	} else {
		t = rv.Type().Elem()
		rv = reflect.ValueOf(tableStruct).Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tags := this.parseTagSetting(field.Tag)
		if _, hasKey := tags["COLUMN"]; hasKey {
			fieldIndex := IndexOf(this.tableFields[tableName], tags["COLUMN"])
			this.structMapping[tableName][tags["COLUMN"]] = DBDataTypeMapping{TableIndex: fieldIndex, Index: i, Field: field.Name, Type: rv.Field(i).Type()}
		} else {
			fieldDatabaseName := ""
			switch field.Name {
			case "ID":
				fieldDatabaseName = "id"
			case "CreatedAt":
				fieldDatabaseName = "created_at"
			case "UpdatedAt":
				fieldDatabaseName = "updated_at"
			case "DeletedAt":
				fieldDatabaseName = "deleted_at"
			}
			if fieldDatabaseName != "" {
				fieldIndex := IndexOf(this.tableFields[tableName], fieldDatabaseName)
				this.structMapping[tableName][fieldDatabaseName] = DBDataTypeMapping{TableIndex: fieldIndex, Index: i, Field: field.Name, Type: rv.Field(i).Type()}
			}
		}
		if _, hasKey := tags["PRIMARY_KEY"]; hasKey {
			fieldIndex := IndexOf(this.tableFields[tableName], tags["PRIMARY_KEY"])
			this.structPrimaryKey[tableName] = DBDataTypeMapping{TableIndex: fieldIndex, Index: i, Field: field.Name, Type: rv.Field(i).Type()}
		}
	}

	// if _, hasKey := this.structPrimaryKey[tableName]; !hasKey {
	// 	return fmt.Errorf("%s %s not defind primary key", this.dbName, tableName)
	// }

	this.tableNameStructMapping[tableName] = tableStruct

	strMapping, err := this.getESIndexMapping(tableStruct)
	if err != nil {
		return err
	}

	indexName := this.getIndexName(tableName)

	res, err := this.esClient.Aliases().Index("_all").Do(this.ctx)
	if err != nil {
		return err
	}

	indexNames := res.IndicesByAlias(indexName)
	if len(indexNames) == 0 {
		newIndexName := fmt.Sprintf("%s-%s", indexName, time.Now().In(time.Local).Format("20060102150405"))
		_, err = this.esClient.CreateIndex(newIndexName).BodyString(strMapping).Do(this.ctx)
		if err != nil {
			return err
		}

		_, err = this.esClient.Alias().Add(newIndexName, indexName).Do(this.ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *SyncMySQLToElasticSearch) getIndexName(tableName string) string {
	return fmt.Sprintf("%s%s%s", this.dbName, this.option.IndexNameSeparator, tableName)
}

func (this *SyncMySQLToElasticSearch) getESIndexMapping(tableStruct interface{}) (string, error) {
	mapping := make(map[string]map[string]string)
	t := reflect.TypeOf(tableStruct)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("estype")
		if tag != "" {
			if mapping[field.Name] == nil {
				mapping[field.Name] = make(map[string]string)
			}
			mapping[field.Name]["type"] = tag
			if tag == "string" {
				tag = field.Tag.Get("esindex")
				if tag != "" {
					mapping[field.Name]["index"] = tag
				}
			}
		} else {
			if _, hasKey := mapping[field.Name]; !hasKey {
				mapping[field.Name] = make(map[string]string)
			}
			switch field.Type.String() {
			case "string":
				mapping[field.Name]["type"] = "text"
			case "uint":
				mapping[field.Name]["type"] = "integer"
			case "int":
				mapping[field.Name]["type"] = "integer"
			case "int64":
				mapping[field.Name]["type"] = "long"
			case "float":
				mapping[field.Name]["type"] = "double"
			case "float32":
				mapping[field.Name]["type"] = "double"
			case "float64":
				mapping[field.Name]["type"] = "double"
			case "bool":
				mapping[field.Name]["type"] = "boolean"
			case "time.Time":
				mapping[field.Name]["type"] = "date"
			case "*time.Time":
				mapping[field.Name]["type"] = "date"
			default:
				// fmt.Println(field.Name, field.Type.String())
				delete(mapping, field.Name)
			}
		}
	}
	byteMapping, err := json.Marshal(mapping)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"settings":{"index.max_result_window":"100000000"},"mappings":{"properties":%s}}`, string(byteMapping)), nil
}

func (this *SyncMySQLToElasticSearch) Sync() error {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = this.addr
	canalCfg.User = this.user
	canalCfg.Password = this.password
	canalCfg.Charset = "utf8mb4"

	includeTableRegex := make([]string, 0)
	for key := range this.tableFields {
		includeTableRegex = append(includeTableRegex, fmt.Sprintf("^%s\\.%s$", this.dbName, key))
	}

	canalCfg.IncludeTableRegex = includeTableRegex
	canalCfg.ExcludeTableRegex = []string{"^mysql\\.ha_health_check$"}

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		return err
	}

	c.SetEventHandler(this)

	err = c.Run()
	if err != nil {
		return err
	}
	return nil
}

func (this *SyncMySQLToElasticSearch) SyncFrom(binlogFile string, binlogPosition uint32) error {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = this.addr
	canalCfg.User = this.user
	canalCfg.Password = this.password
	canalCfg.Dump.ExecutionPath = ""

	includeTableRegex := make([]string, 0)
	for key := range this.tableFields {
		includeTableRegex = append(includeTableRegex, fmt.Sprintf("^%s\\.%s$", this.dbName, key))
	}

	canalCfg.IncludeTableRegex = includeTableRegex

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		return err
	}

	c.SetEventHandler(this)

	err = c.RunFrom(mysql.Position{Name: binlogFile, Pos: binlogPosition})
	if err != nil {
		return err
	}
	return nil
}

func (this *SyncMySQLToElasticSearch) OnRow(rowsEvent *canal.RowsEvent) error {
	step := 1
	if rowsEvent.Action == "update" {
		step = 2
	}

	for i := 0; i < len(rowsEvent.Rows); i = i + step {
		intfc := this.tableNameStructMapping[rowsEvent.Table.Name]
		sType := reflect.TypeOf(intfc)
		indexName := this.getIndexName(rowsEvent.Table.Name)

		var row []interface{}
		if rowsEvent.Action == "update" {
			row = rowsEvent.Rows[i+1]
		} else if rowsEvent.Action == "insert" {
			row = rowsEvent.Rows[i]
		} else if rowsEvent.Action == "delete" {
			row = rowsEvent.Rows[i]
		} else {
			row = rowsEvent.Rows[i]
		}

		sInstance := reflect.New(sType).Elem()
		for _, column := range this.structMapping[rowsEvent.Table.Name] {
			switch column.Type.String() {
			case "string":
				switch row[column.TableIndex].(type) {
				case []uint8:
					sInstance.Field(column.Index).SetString(string(row[column.TableIndex].([]uint8)))
				case string:
					sInstance.Field(column.Index).SetString(row[column.TableIndex].(string))
				case nil:
					sInstance.Field(column.Index).SetString("")
				default:
					typeof := reflect.TypeOf(row[column.TableIndex])
					log.Debug(fmt.Sprint(row[column.TableIndex], typeof.String()))
				}
			case "uint":
				switch row[column.TableIndex].(type) {
				case uint:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(val.Uint())
				case uint8:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(val.Uint())
				case uint16:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(val.Uint())
				case uint32:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(val.Uint())
				case uint64:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(val.Uint())
				case int:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(uint64(val.Int()))
				case int8:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(uint64(val.Int()))
				case int16:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(uint64(val.Int()))
				case int32:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(uint64(val.Int()))
				case int64:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetUint(uint64(val.Int()))
				default:
					typeof := reflect.TypeOf(row[column.TableIndex])
					log.Debug(fmt.Sprint(row[column.TableIndex], typeof.String()))
				}
			case "int64", "int":
				switch row[column.TableIndex].(type) {
				case uint:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(int64(val.Uint()))
				case uint8:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(int64(val.Uint()))
				case uint16:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(int64(val.Uint()))
				case uint32:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(int64(val.Uint()))
				case uint64:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(int64(val.Uint()))
				case int:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(val.Int())
				case int8:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(val.Int())
				case int16:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(val.Int())
				case int32:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(val.Int())
				case int64:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetInt(val.Int())
				case string:
					val := reflect.ValueOf(row[column.TableIndex])
					intVal, err := strconv.ParseInt(val.String(), 10, 64)
					if err != nil {
						log.Debug(err)
					}
					sInstance.Field(column.Index).SetInt(intVal)
				default:
					typeof := reflect.TypeOf(row[column.TableIndex])
					log.Debug(fmt.Sprint(row[column.TableIndex], typeof.String()))
				}
			case "float64", "float":
				switch row[column.TableIndex].(type) {
				case float32:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetFloat(val.Float())
				case float64:
					val := reflect.ValueOf(row[column.TableIndex])
					sInstance.Field(column.Index).SetFloat(val.Float())
				default:
					typeof := reflect.TypeOf(row[column.TableIndex])
					log.Debug(fmt.Sprint(row[column.TableIndex], typeof.String()))
				}
			case "bool":
				switch row[column.TableIndex].(type) {
				case uint8:
					val := reflect.ValueOf(row[column.TableIndex])
					if val.Uint() != 0 {
						sInstance.Field(column.Index).SetBool(true)
					} else {
						sInstance.Field(column.Index).SetBool(false)
					}
				case int8:
					val := reflect.ValueOf(row[column.TableIndex])
					if val.Int() != 0 {
						sInstance.Field(column.Index).SetBool(true)
					} else {
						sInstance.Field(column.Index).SetBool(false)
					}
				case int64:
					val := reflect.ValueOf(row[column.TableIndex])
					if val.Int() != 0 {
						sInstance.Field(column.Index).SetBool(true)
					} else {
						sInstance.Field(column.Index).SetBool(false)
					}
				default:
					typeof := reflect.TypeOf(row[column.TableIndex])
					log.Debug(fmt.Sprint(row[column.TableIndex], typeof.String()))
				}
			case "time.Time":
				switch row[column.TableIndex].(type) {
				case string:
					layout := "2006-01-02 15:04:05"
					t, errParseInLocation := time.ParseInLocation(layout, row[column.TableIndex].(string), time.Local)
					if errParseInLocation == nil {
						val := reflect.ValueOf(t)
						sInstance.Field(column.Index).Set(val)
					}
				default:
					typeof := reflect.TypeOf(row[column.TableIndex])
					log.Debug(fmt.Sprint(row[column.TableIndex], typeof.String()))
				}
			case "*time.Time":
				switch row[column.TableIndex].(type) {
				case string:
					layout := "2006-01-02 15:04:05"
					t, errParseInLocation := time.ParseInLocation(layout, row[column.TableIndex].(string), time.Local)
					if errParseInLocation == nil {
						val := reflect.ValueOf(&t)
						sInstance.Field(column.Index).Set(val)
					}
				case nil:
					// val := reflect.ValueOf(nil)
					// sInstance.Field(column.Index).Set(val)
				default:
					typeof := reflect.TypeOf(row[column.TableIndex])
					log.Debug(fmt.Sprint(row[column.TableIndex], typeof.String()))
				}
			default:
				log.Debug(column.Type.String())
			}
		}

		itf := sInstance.Interface()
		rv := reflect.ValueOf(itf)

		primaryKey := ""
		if _, hasPrimaryKey := this.structPrimaryKey[rowsEvent.Table.Name]; hasPrimaryKey {
			primaryKey = fmt.Sprint(rv.Field(this.structPrimaryKey[rowsEvent.Table.Name].Index).Interface())
		} else {
			primaryKeys := make([]string, 0)
			for i := 0; i < rv.NumField(); i++ {
				if rv.Field(i).Type().Kind() == reflect.Uint {
					primaryKeys = append(primaryKeys, fmt.Sprint(rv.Field(i).Interface()))
				}
			}
			primaryKey = strings.Join(primaryKeys, ",")
		}

		this.addToESQueue(rowsEvent.Action, indexName, primaryKey, itf)
	}

	return nil
}

func (this *SyncMySQLToElasticSearch) OnPosSynced(pos mysql.Position, _ mysql.GTIDSet, force bool) error {
	if this.onPosSynced != nil {
		this.onPosSynced(pos.Name, pos.Pos, force)
	}
	return nil
}

func (this *SyncMySQLToElasticSearch) getObjectValues(object interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	t := reflect.TypeOf(object)
	v := reflect.ValueOf(object)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		result[field.Name] = v.Field(i).Interface()
	}
	return result
}
