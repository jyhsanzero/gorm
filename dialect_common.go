package gorm

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var keyNameRegex = regexp.MustCompile("[^a-zA-Z0-9]+")

// DefaultForeignKeyNamer contains the default foreign key name generator method
type DefaultForeignKeyNamer struct {
}

type commonDialect struct {
	db SQLCommon
	DefaultForeignKeyNamer
}

func init() {
	RegisterDialect("common", &commonDialect{})
}

func (commonDialect) GetName() string {
	return "common"
}

func (s *commonDialect) SetDB(db SQLCommon) {
	s.db = db
}

func (commonDialect) BindVar(i int) string {
	return "$$$" // ?
}

func (commonDialect) Quote(key string) string {
	return fmt.Sprintf(`"%s"`, key)
}

func (s *commonDialect) fieldCanAutoIncrement(field *StructField) bool {
	if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		return strings.ToLower(value) != "false"
	}
	return field.IsPrimaryKey
}

func (s *commonDialect) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialect(field, s)

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "BOOLEAN"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			if s.fieldCanAutoIncrement(field) {
				sqlType = "INTEGER AUTO_INCREMENT"
			} else {
				sqlType = "INTEGER"
			}
		case reflect.Int64, reflect.Uint64:
			if s.fieldCanAutoIncrement(field) {
				sqlType = "BIGINT AUTO_INCREMENT"
			} else {
				sqlType = "BIGINT"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "FLOAT"
		case reflect.String:
			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("VARCHAR(%d)", size)
			} else {
				sqlType = "VARCHAR(65532)"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "TIMESTAMP"
			}
		default:
			if _, ok := dataValue.Interface().([]byte); ok {
				if size > 0 && size < 65532 {
					sqlType = fmt.Sprintf("BINARY(%d)", size)
				} else {
					sqlType = "BINARY(65532)"
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) for commonDialect", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s commonDialect) HasIndex(tableName string, indexName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = ? AND table_name = ? AND index_name = ?", currentDatabase, tableName, indexName).Scan(&count)
	return count > 0
}

func (s commonDialect) RemoveIndex(tableName string, indexName string) error {
	_, err := s.db.Exec(fmt.Sprintf("DROP INDEX %v", indexName))
	return err
}

func (s commonDialect) HasForeignKey(tableName string, foreignKeyName string) bool {
	return false
}

func (s commonDialect) HasTable(tableName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?", currentDatabase, tableName).Scan(&count)
	return count > 0
}

func (s commonDialect) HasColumn(tableName string, columnName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND column_name = ?", currentDatabase, tableName, columnName).Scan(&count)
	return count > 0
}

func (s commonDialect) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v", tableName, columnName, typ))
	return err
}

func (s commonDialect) CurrentDatabase() (name string) {
	s.db.QueryRow("SELECT DATABASE()").Scan(&name)
	return
}

func (commonDialect) LimitAndOffsetSQL(limit, offset interface{}) (sql string) {
	if limit != nil {
		if parsedLimit, err := strconv.ParseInt(fmt.Sprint(limit), 0, 0); err == nil && parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)
		}
	}
	if offset != nil {
		if parsedOffset, err := strconv.ParseInt(fmt.Sprint(offset), 0, 0); err == nil && parsedOffset >= 0 {
			sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
		}
	}
	return
}

func (commonDialect) SelectFromDummyTable() string {
	return ""
}

func (commonDialect) LastInsertIDOutputInterstitial(tableName, columnName string, columns []string) string {
	return ""
}

func (commonDialect) LastInsertIDReturningSuffix(tableName, columnName string) string {
	return ""
}

func (commonDialect) DefaultValueStr() string {
	return "DEFAULT VALUES"
}

// BuildKeyName returns a valid key name (foreign key, index key) for the given table, field and reference
func (DefaultForeignKeyNamer) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := fmt.Sprintf("%s_%s_%s", kind, tableName, strings.Join(fields, "_"))
	keyName = keyNameRegex.ReplaceAllString(keyName, "_")
	return keyName
}

// NormalizeIndexAndColumn returns argument's index name and column name without doing anything
func (commonDialect) NormalizeIndexAndColumn(indexName, columnName string) (string, string) {
	return indexName, columnName
}

// IsByteArrayOrSlice returns true of the reflected value is an array or slice
func IsByteArrayOrSlice(value reflect.Value) bool {
	return (value.Kind() == reflect.Array || value.Kind() == reflect.Slice) && value.Type().Elem() == reflect.TypeOf(uint8(0))
}

/*For firebirdsql*/
func (s commonDialect) CreateAutoIncrementTrigger(tableName string, column string) {
	//Firebirdsql doesn't support the key of "AUTO_INCREMENT",if you want create an auto increment column,
	//you should create a trigger for this column.
	autoIncrementSql := "CREATE TRIGGER %v FOR %v ACTIVE BEFORE INSERT POSITION 0 AS BEGIN IF (NEW.%v IS NULL) THEN NEW.%v = GEN_ID(%v,1); END"
	triggerName := s.BuildKeyName(tableName, column, "BI")
	generatorName := s.BuildKeyName("GEN", tableName, column)
	if s.HasGeneratorName(generatorName) {
		return
	}
	s.CreateGeneratorName(generatorName)
	s.db.Exec(fmt.Sprintf(autoIncrementSql, triggerName, tableName, column, column, generatorName))
}

func (s commonDialect) RemoveTrigger(tableName string, column string) {
	triggerName := s.BuildKeyName(tableName, column, "BI")
	generatorName := s.BuildKeyName("GEN", tableName, column)
	s.RemoveGeneratorName(generatorName)
	s.db.Exec(fmt.Sprintf("DROP TRIGGER %v", triggerName))
}

func (s commonDialect) HasGeneratorName(generatorName string) bool {
	//Check the generatorName had been defined or not.
	var count int
	//generatorName should be capitalized
	generatorName = strings.ToUpper(generatorName)
	s.db.QueryRow("SELECT COUNT(*) FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME=?", generatorName).Scan(&count)
	return count > 0
}

func (s commonDialect) CreateGeneratorName(generatorName string) {
	//Before you create a trigger,you should insure that the generator had been defined.
	if !s.HasGeneratorName(generatorName) {
		s.db.Exec(fmt.Sprintf("CREATE GENERATOR %v;", generatorName))
	}
}

func (s commonDialect) RemoveGeneratorName(generatorName string) {
	s.db.Exec(fmt.Sprintf("DROP GENERATOR %v;", generatorName))
}
