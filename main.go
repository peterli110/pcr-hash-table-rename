package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var originalDBPath, hashedDBPath, generatedDBPath, filter string
var generateHashJson bool

var originalDBMap = map[string][][]string{}
var hashedDBMap = map[string][][]string{}
var tableMapping = map[string]string{}
var filterTables = map[string]struct{}{}

var numericRegex = regexp.MustCompile(`^\d+(\.\d+)?$`)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "pcr-hash-table-rename",
		Short: "PCR Hash Table Rename",
		Long: `Generate a new database with human-readable table names from a hashed database in Princess Connect Re:Dive.
                Complete documentation is available at https://github.com/peterli110/pcr-hash-table-rename`,
		Run: func(cmd *cobra.Command, args []string) {
			run(originalDBPath, hashedDBPath, generatedDBPath, generateHashJson)
		},
	}

	rootCmd.PersistentFlags().StringVarP(&originalDBPath, "originalDBPath", "r", "", "REQUIRED: Path to the original (human-readable one) database")
	rootCmd.PersistentFlags().StringVarP(&hashedDBPath, "hashedDBPath", "n", "", "REQUIRED: Path to the hashed (latest) database")
	rootCmd.PersistentFlags().StringVarP(&generatedDBPath, "generatedDBPath", "g", "jp_fixed.db", "OPTIONAL: Path to the new database, default to jp_fixed.db")
	rootCmd.PersistentFlags().BoolVarP(&generateHashJson, "generateTableMapping", "t", false, "OPTIONAL: Generate a mapping of raw table name -> hash table name in JSON")
	rootCmd.PersistentFlags().StringVarP(&filter, "filter", "f", "", "OPTIONAL: Use a file to generate a new database with only the tables in the file")
	_ = rootCmd.MarkPersistentFlagRequired("originalDBPath")
	_ = rootCmd.MarkPersistentFlagRequired("hashedDBPath")

	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

func run(originalDBPath string, hashedDBPath string, generatedDBPath string, generateHashJson bool) {
	if filter != "" {
		readFilterFile()
	}
	originalDB, err := sql.Open("sqlite3", originalDBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer originalDB.Close()

	hashedDB, err := sql.Open("sqlite3", hashedDBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer hashedDB.Close()

	readFromDB(originalDB, originalDBMap, true)
	readFromDB(hashedDB, hashedDBMap, false)

	newDB, err := sql.Open("sqlite3", generatedDBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer newDB.Close()

	for t, v := range originalDBMap {
		if filter != "" {
			if _, ok := filterTables[t]; !ok {
				continue
			}
		}
		if hashedTable, ok := findMatchingTable(v, hashedDB, t); ok {
			tableMapping[t] = hashedTable
			copyData(originalDB, hashedDB, newDB, t, hashedTable)
		} else {
			log.Println("no matching table for", t)
		}
	}

	if generateHashJson {
		writeJson()
	}

	log.Println("Done!")
}

func readFromDB(db *sql.DB, dbMap map[string][][]string, filterV1Table bool) {
	tables := getTableNames(db, filterV1Table)

	for _, table := range tables {
		dbMap[table] = getFirstNRows(db, table, 1)
	}
}

func getTableNames(db *sql.DB, filterV1Tables bool) []string {
	tables := make([]string, 0)
	query := "SELECT name FROM sqlite_master WHERE type='table';"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error querying database: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		// ignore the sqlite_stat1 table because row data is also hashed
		if name == "sqlite_stat1" {
			continue
		}
		// ignore the new hashed v1_ tables
		if strings.HasPrefix(name, "v1_") {
			if !filterV1Tables {
				tables = append(tables, name)
			}
		} else {
			tables = append(tables, name)
		}
	}

	return tables
}

func findMatchingTable(values [][]string, hashedDB *sql.DB, table string) (string, bool) {
	if len(values) == 0 {
		return "", false
	}
	for t, v := range hashedDBMap {
		if len(v) == 0 {
			continue
		}
		if compareData(values, v) {
			// these 2 tables have the same data but different number of rows
			// looks like unit_unique_equip is deprecated and there are only 183 rows
			if table == "unit_unique_equipment" || table == "unit_unique_equip" {
				rowsCount := countRowsInTable(hashedDB, t)
				if (table == "unit_unique_equipment" && rowsCount < 200) || (table == "unit_unique_equip" && rowsCount > 200) {
					continue
				}
			}
			return t, true
		}
	}

	return "", false
}

func getFirstNRows(db *sql.DB, tableName string, n int) [][]string {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, n)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error querying database in table %s: %v", tableName, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns in table %s: %v", tableName, err)
	}

	var tableData [][]string
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err = rows.Scan(columnPointers...); err != nil {
			log.Fatalf("Error scanning row in table %s: %v", tableName, err)
		}

		var rowValues []string
		for _, col := range columns {
			rowValues = append(rowValues, fmt.Sprintf("%v", col))
		}
		tableData = append(tableData, rowValues)
	}

	return tableData
}

func compareData(data1, data2 [][]string) bool {
	if len(data1) != len(data2) {
		return false
	}

	for i := range data1 {
		if !reflect.DeepEqual(data1[i], data2[i]) {
			return false
		}
	}
	return true
}

func copyData(originalDB, hashedDB, newDB *sql.DB, origTable, hashedTable string) {
	// get the CREATE TABLE statement for the original table
	createStmt, err := getCreateTableStatement(originalDB, origTable)
	if err != nil {
		log.Fatalf("Error getting CREATE TABLE statement for table %s: %v", origTable, err)
	}
	log.Println(createStmt)

	// create the new table in the new database
	_, err = newDB.Exec(createStmt)
	if err != nil {
		log.Fatalf("Error creating table %s in new database: %v", origTable, err)
	}

	// fetch data from the hashed table
	hashedData, err := getAllData(hashedDB, hashedTable)
	if err != nil {
		log.Fatalf("Error fetching data from hashed table %s: %v", hashedTable, err)
	}

	// copy data row by row to the new table
	for _, row := range hashedData {
		insertStmt := createInsertStatement(origTable, row)
		log.Println(insertStmt)
		_, err = newDB.Exec(insertStmt)
		if err != nil {
			log.Fatalf("Error inserting data into new table:", err)
		}
	}
}

func getAllData(db *sql.DB, tableName string) ([][]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var tableData [][]string
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		var rowValues []string
		for _, col := range columns {
			rowValues = append(rowValues, fmt.Sprintf("%v", col))
		}
		tableData = append(tableData, rowValues)
	}

	return tableData, nil
}

func countRowsInTable(db *sql.DB, tableName string) int {
	var count int

	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		log.Fatalf("Error counting rows in table %s: %v", tableName, err)
	}

	return count
}

func getCreateTableStatement(db *sql.DB, tableName string) (string, error) {
	query := "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
	var createStmt string
	row := db.QueryRow(query, tableName)
	err := row.Scan(&createStmt)
	if err != nil {
		return "", err
	}
	return createStmt, nil
}

func createInsertStatement(tableName string, rowData []string) string {
	var formattedValues []string

	for _, value := range rowData {
		formattedValues = append(formattedValues, formatValueByType(value))
	}

	values := strings.Join(formattedValues, ", ")
	return fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, values)
}

func formatValueByType(value string) string {
	if isNumeric(value) {
		return value
	}

	escapedValue := strings.ReplaceAll(value, "'", "''")
	return fmt.Sprintf("'%s'", escapedValue)
}

func isNumeric(s string) bool {
	return numericRegex.MatchString(s)
}

func writeJson() {
	jsonData, err := json.MarshalIndent(tableMapping, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Create("table_mapping.json")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		log.Fatal(err)
	}
}

func readFilterFile() {
	file, err := os.Open(filter)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		if text != "" {
			filterTables[text] = struct{}{}
		}
	}

	if err = scanner.Err(); err != nil {
		log.Fatalf("Error reading filter file: %v", err)
	}
}
