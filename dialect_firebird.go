package gorm

/*For firebirdsql*/

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var firebirdsqlIndexRegex = regexp.MustCompile(`^(.+)\((\d+)\)$`)

type firebirdsql struct {
	commonDialect
}

func init() {
	RegisterDialect("firebirdsql", &firebirdsql{})
}

//finish checked
func (firebirdsql) GetName() string {
	return "firebirdsql"
}

//Use dialect_common
/*func (s *commonDialect) SetDB(db SQLCommon) {
	s.db = db
}*/

//Use dialect_common
/*func (commonDialect) BindVar(i int) string {
	return "$$$" // ?
}*/

func (firebirdsql) Quote(key string) string {
	return strings.ToUpper(key)
}

//Use dialect_common
/*func (s *commonDialect) fieldCanAutoIncrement(field *StructField) bool {
	if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		return strings.ToLower(value) != "false"
	}
	return field.IsPrimaryKey
}*/

// Get Data Type for firbirdsql Dialect
func (s *firebirdsql) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialect(field, s)
	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool, reflect.Int8, reflect.Uint8:
			sqlType = "SMALLINT"
		case reflect.Int, reflect.Int16, reflect.Int32:
			sqlType = "INTEGER"
		case reflect.Uint, reflect.Uint16, reflect.Uint32:
			sqlType = "INTEGER"
		case reflect.Int64, reflect.Uint64:
			sqlType = "BIGINT"
		case reflect.Float32:
			sqlType = "FLOAT"
		case reflect.Float64:
			sqlType = "DOUBLE PRECISION"
		case reflect.String:
			if size > 0 && size < 32765 {
				sqlType = fmt.Sprintf("VARCHAR(%d)", size)
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				if _, ok := field.TagSettings["NOT NULL"]; ok || field.IsPrimaryKey {
					sqlType = "TIMESTAMP NOT NULL"
				} else {
					sqlType = "TIMESTAMP"
				}
			}
		default:
			if IsByteArrayOrSlice(dataValue) {
				if size > 0 && size < 65532 {
					sqlType = "BLOB"
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) in field %s for firebirdsql", dataValue.Type().Name(), dataValue.Kind().String(), field.Name))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s firebirdsql) HasIndex(tableName string, indexName string) bool {
	var count int
	//tableName and indexName should be capitalized
	tableName = strings.ToUpper(tableName)
	indexName = strings.ToUpper(indexName)
	s.db.QueryRow("SELECT COUNT(*) FROM RDB$INDICES WHERE RDB$RELATION_NAME=? AND RDB$INDEX_NAME=?", tableName, indexName).Scan(&count)
	return count > 0
}

func (s firebirdsql) RemoveIndex(tableName string, indexName string) error {
	_, err := s.db.Exec(fmt.Sprintf("DROP INDEX %v", indexName))
	return err
}

func (s firebirdsql) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	//tableName and foreignKeyName should be capitalized
	tableName = strings.ToUpper(tableName)
	foreignKeyName = strings.ToUpper(foreignKeyName)
	s.db.QueryRow("SELECT COUNT(*) FROM RDB$RELATION_CONSTRAINTS WHERE RDB$RELATION_NAME=? AND RDB$CONSTRAINT_NAME=? AND RDB$CONSTRAINT_TYPE='FOREIGN KEY'", tableName, foreignKeyName).Scan(&count)
	return count > 0
}

func (s firebirdsql) HasTable(tableName string) bool {
	var count int
	//tableName  should be capitalized
	tableName = strings.ToUpper(tableName)
	s.db.QueryRow("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME=?", tableName).Scan(&count)
	return count > 0
}

func (s firebirdsql) HasColumn(tableName string, columnName string) bool {
	var count int
	//tableName and columnName should be capitalized
	tableName = strings.ToUpper(tableName)
	columnName = strings.ToUpper(columnName)
	s.db.QueryRow("SELECT COUNT(*) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME=? AND RDB$FIELD_NAME=?", tableName, columnName).Scan(&count)
	return count > 0
}

func (s firebirdsql) ModifyColumn(tableName string, columnName string, typ string) error {
	// ModifyColumn modify column's type,only type.
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v", tableName, columnName, typ))
	return err
}

/*func (s firebirdsql) ModifyColumn(tableName string, columnName string, typ string) error {
	// If you want modify column's other attribute.
	// You can set typ like "TYPE VARCHAR(128)" to modify the column's type.
	// You can set typ like "TO new_col_name" to modify the column's name.
	// You can set typ like "POSITION new_col_position" to modify the column's position.
	// You can set typ like "SET DEFAULT default_value" to modify the column's default value.
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v %v", tableName, columnName, typ))
	return err
}*/

func (s firebirdsql) CurrentDatabase() (name string) {
	//It will return the full path of this databases,eg:"/firebirddatabases/database.fdb"
	s.db.QueryRow("SELECT MON$DATABASE_NAME FROM MON$DATABASE").Scan(&name)
	return
}

func (s firebirdsql) LimitAndOffsetSQL(limit, offset interface{}) (sql string) {
	//Firebirdsql doesn't support the keywords like "LIMIT" and "OFFSET".
	if offset == nil && limit == nil {
		return ""
	}
	var parsedOffset, parsedLimit int64
	var err error
	if offset != nil {
		if parsedOffset, err = strconv.ParseInt(fmt.Sprint(offset), 0, 0); err == nil && parsedOffset > 0 {
			parsedOffset = parsedOffset + 1
		} else {
			parsedOffset = 0
		}
	}
	if limit != nil {
		if parsedLimit, err = strconv.ParseInt(fmt.Sprint(limit), 0, 0); err == nil && parsedLimit > 0 {
			if parsedOffset > 0 {
				parsedLimit = parsedLimit + parsedOffset - 1
			}

		}
	}
	if parsedOffset > 0 && parsedLimit > 0 {
		sql += fmt.Sprintf(" ROWS %d TO %d", parsedOffset, parsedLimit)
	} else if parsedOffset > 0 {
		sql += fmt.Sprintf(" ROWS %d TO %d", parsedOffset, 10000)
	} else if parsedLimit >= 0 {
		sql += fmt.Sprintf(" ROWS %d", parsedLimit)
	} else {
		sql += ""
	}
	return
}

func (firebirdsql) SelectFromDummyTable() string {
	return "FROM RDB$DATABASE"
}

//Use dialect_common
/*func (commonDialect) LastInsertIDOutputInterstitial(tableName, columnName string, columns []string) string {
	return ""
}*/

//Use dialect_common
/*func (commonDialect) LastInsertIDReturningSuffix(tableName, columnName string) string {
	return ""
}*/

func (firebirdsql) DefaultValueStr() string {
	return "DEFAULT VALUES"
}

func (s firebirdsql) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := fmt.Sprintf("%s_%s", kind, strings.Join(fields, "_"))
	keyName = keyNameRegex.ReplaceAllString(keyName, "_")
	keyName = strings.ToUpper(keyName)
	return keyName
}

//Use dialect_common
/*func (commonDialect) NormalizeIndexAndColumn(indexName, columnName string) (string, string) {
	return indexName, columnName
}*/

//Use dialect_common
/*func IsByteArrayOrSlice(value reflect.Value) bool {
	return (value.Kind() == reflect.Array || value.Kind() == reflect.Slice) && value.Type().Elem() == reflect.TypeOf(uint8(0))
}*/

func (s firebirdsql) CreateAutoIncrementTrigger(tableName string, column string) {
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

func (s firebirdsql) RemoveTrigger(tableName string, column string) {
	triggerName := s.BuildKeyName(tableName, column, "BI")
	generatorName := s.BuildKeyName("GEN", tableName, column)
	s.RemoveGeneratorName(generatorName)
	s.db.Exec(fmt.Sprintf("DROP TRIGGER %v", triggerName))
}

func (s firebirdsql) HasGeneratorName(generatorName string) bool {
	//Check the generatorName had been defined or not.
	var count int
	//generatorName should be capitalized
	generatorName = strings.ToUpper(generatorName)
	s.db.QueryRow("SELECT COUNT(*) FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME=?", generatorName).Scan(&count)
	return count > 0
}

func (s firebirdsql) CreateGeneratorName(generatorName string) {
	//Before you create a trigger,you should insure that the generator had been defined.
	if !s.HasGeneratorName(generatorName) {
		s.db.Exec(fmt.Sprintf("CREATE GENERATOR %v;", generatorName))
	}
}

func (s firebirdsql) RemoveGeneratorName(generatorName string) {
	s.db.Exec(fmt.Sprintf("DROP GENERATOR %v;", generatorName))
}
