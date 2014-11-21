package dynamostore

import (
	"github.com/flowhealth/goamz/aws"
	"github.com/flowhealth/goamz/dynamodb"
	"github.com/flowhealth/goannoying"
	"github.com/flowhealth/gocontract/contract"
	log "github.com/flowhealth/logrus"
	"time"
)

const (
	PrimaryKeyName                      = "Key"
	TableStatusActive                   = "ACTIVE"
	TableStatusCreating                 = "CREATING"
	DefaultTableCreateCheckTimeout      = "20s"
	DefaultTableCreateCheckPollInterval = "3s"
	DefaultReadCapacity                 = 1
	DefaultWriteCapacity                = 1
)

var (
	DefaultRegion = aws.USWest2
)

/*
 Repository
*/

type IKeyAttrStore interface {
	Get(string) (map[string]*dynamodb.Attribute, *TError)
	All(startFromKey string, limit int) ([]map[string]*dynamodb.Attribute, string, *TError)
	Save(string, ...dynamodb.Attribute) *TError
	Delete(string) *TError
	Init() *TError
	Destroy() *TError
}

type TKeyAttrStore struct {
	dynamoServer *dynamodb.Server
	table        *dynamodb.Table
	tableDesc    *dynamodb.TableDescriptionT
	cfg          *TConfig
}

type TConfig struct {
	Name                         string
	ReadCapacity, WriteCapacity  int64
	Region                       aws.Region
	TableCreateCheckTimeout      string
	TableCreateCheckPollInterval string
}

func MakeDefaultConfig(name string) *TConfig {
	return MakeConfig(name, DefaultReadCapacity, DefaultWriteCapacity, DefaultRegion, DefaultTableCreateCheckTimeout, DefaultTableCreateCheckPollInterval)
}

func MakeConfig(name string, readCapacity, writeCapacity int64, region aws.Region, tableCreateTimeout, tableCreatePoll string) *TConfig {
	return &TConfig{name, readCapacity, writeCapacity, region, tableCreateTimeout, tableCreatePoll}
}

func MakeKeyAttrStore(cfg *TConfig) IKeyAttrStore {
	var (
		auth aws.Auth
		pk   dynamodb.PrimaryKey
	)
	tableDesc := keyAttrTableDescription(cfg.Name, cfg.ReadCapacity, cfg.WriteCapacity)
	contract.RequireNoErrors(
		func() (err error) {
			auth, err = aws.GetAuth(auth.AccessKey, auth.SecretKey, auth.Token(), auth.Expiration())
			return
		},
		func() (err error) {
			pk, err = tableDesc.BuildPrimaryKey()
			return
		})
	dynamo := dynamodb.Server{auth, cfg.Region}
	table := dynamo.NewTable(cfg.Name, pk)
	repo := &TKeyAttrStore{&dynamo, table, tableDesc, cfg}
	repo.Init()
	return repo
}

/*
	DynamoDB Table Configuration
*/

func keyAttrTableDescription(name string, readCapacity, writeCapacity int64) *dynamodb.TableDescriptionT {
	return &dynamodb.TableDescriptionT{
		TableName: name,
		AttributeDefinitions: []dynamodb.AttributeDefinitionT{
			dynamodb.AttributeDefinitionT{PrimaryKeyName, "S"},
		},
		KeySchema: []dynamodb.KeySchemaT{
			dynamodb.KeySchemaT{PrimaryKeyName, "HASH"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
			ReadCapacityUnits:  readCapacity,
			WriteCapacityUnits: writeCapacity,
		},
	}
}

func (self *TKeyAttrStore) findTableByName(name string) bool {
	log.WithField("table", name).Debug("Searching table")
	tables, err := self.dynamoServer.ListTables()
	log.WithField("talbes", tables).Debug("Got table list")
	contract.RequireNoErrorf(err, "Failed to lookup table %v", err)
	for _, t := range tables {
		if t == name {
			log.WithField("table", name).Debug("Found table")
			return true
		}
	}
	log.WithField("table", name).Debug("Table isn't found")
	return false
}

func (self *TKeyAttrStore) Init() *TError {
	tableName := self.tableDesc.TableName
	log.WithField("table", tableName).Debug("Initializing Store")
	tableExists := self.findTableByName(tableName)
	if tableExists {
		log.WithField("table", tableName).Debug("Table exists, skipping init")
		self.waitUntilTableIsActive(tableName)
		log.WithField("table", tableName).Debug("Table is active")
		return nil
	} else {
		log.WithField("table", tableName).Debug("Creating Store")
		status, err := self.dynamoServer.CreateTable(*self.tableDesc)
		if err != nil {
			log.WithField("error", err.Error()).Fatal("Table intialization, cannot proceed")
			return InitGeneralErr
		}
		if status == TableStatusCreating {
			log.WithField("table", tableName).Debug("Waiting until table becomes active")
			self.waitUntilTableIsActive(tableName)
			log.WithField("table", tableName).Debug("Table become active")
			return nil
		}
		if status == TableStatusActive {
			log.WithField("table", tableName).Debug("Table is active")
			return nil
		}
		log.WithFields(log.Fields{
			"table":  tableName,
			"status": status,
		}).Fatal("Table intialization, cannot proceed")
		return InitUnknownStatusErr
	}
}

func (self *TKeyAttrStore) waitUntilTableIsActive(table string) {
	checkTimeout, _ := time.ParseDuration(self.cfg.TableCreateCheckTimeout)
	checkInterval, _ := time.ParseDuration(self.cfg.TableCreateCheckPollInterval)
	ok, err := annoying.WaitUntil("table active", func() (status bool, err error) {
		status = false
		desc, err := self.dynamoServer.DescribeTable(table)
		if err != nil {
			return
		}
		if desc.TableStatus == TableStatusActive {
			status = true
			return
		}
		return
	}, checkInterval, checkTimeout)
	if !ok {
		log.WithField("error", err.Error()).Fatal("Wait until table is active")
	}
}

func (self *TKeyAttrStore) Destroy() *TError {
	log.Info("Destroying tables")
	tableExists := self.findTableByName(self.tableDesc.TableName)
	if !tableExists {
		log.WithField("table", self.tableDesc.TableName).Info("Table doesn't exists, skipping deletion")
		return nil
	} else {
		_, err := self.dynamoServer.DeleteTable(*self.tableDesc)
		if err != nil {
			log.WithFields(log.Fields{
				"table": self.tableDesc.TableName,
				"error": err.Error(),
			}).Fatal("Can't destroy table")
			return DestroyGeneralErr
		}
		log.WithField("table", self.tableDesc.TableName).Debug("Table deleted successfully")
	}
	return nil
}

func (self *TKeyAttrStore) Delete(key string) *TError {
	log.WithField("key", key).Debug("Deleting item")
	ok, err := self.table.DeleteItem(&dynamodb.Key{HashKey: key})
	if ok {
		log.WithField("key", key).Debug("Succeed delete item")
		return nil
	} else {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("Failed to delete item")
		return DeleteErr
	}
}

func (self *TKeyAttrStore) Save(key string, attrs ...dynamodb.Attribute) *TError {
	log.WithField("key", key).Debug("Saving item")
	if ok, err := self.table.PutItem(key, "", attrs); ok {
		return nil
	} else {
		log.WithField("error", err.Error()).Error("Failed to save")
		return SaveErr
	}
}

func (self *TKeyAttrStore) Get(key string) (map[string]*dynamodb.Attribute, *TError) {
	contract.Requiref(key != "", "Empty key is not allowed")

	log.WithField("key", key).Debug("Getting item")
	if attrMap, err := self.table.GetItem(makePrimaryKey(key)); err != nil {
		if err == dynamodb.ErrNotFound {
			return nil, NotFoundErr
		} else {
			log.WithField("error", err.Error()).Error("Failed to get an item")
			return nil, LookupErr
		}
	} else {
		log.WithFields(log.Fields{
			"key":        key,
			"attributes": attrMap,
		}).Debug("Success")
		return attrMap, nil
	}
}

func (self *TKeyAttrStore) All(startFromKey string, limit int) ([]map[string]*dynamodb.Attribute, string, *TError) {
	var pk *dynamodb.Key
	if startFromKey != "" {
		pk = makePrimaryKey(startFromKey)
	}
	nilAttrComparisons := []dynamodb.AttributeComparison{}
	if attrMaps, lastKey, err := self.table.ScanPartialLimit(nilAttrComparisons, pk, int64(limit)); err != nil {
		log.WithField("error", err.Error()).Error("Failed to perform scan")
		return nil, "", LookupErr
	} else {
		log.WithField("count", len(attrMaps)).Debug("Succeed scan fetch")
		if lastKey != nil {
			return attrMaps, lastKey.HashKey, nil
		} else {
			return attrMaps, "", nil
		}
	}
}

func makePrimaryKey(key string) *dynamodb.Key {
	return &dynamodb.Key{HashKey: key}
}
