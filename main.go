package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	_ "github.com/mattn/go-sqlite3"
)

var originalDBPath, hashedDBPath, generatedDBPath, filter string
var generateHashJson bool
var tableMapping = map[string]string{}
var filterTables = map[string]struct{}{}

// TableInfo holds metadata and sample data for a single table.
type TableInfo struct {
	Name        string
	ColumnCount int
	RowCount    int
	FirstNRows  [][]string // first N rows, values as strings
}

// MatchResult records a successful match between an original and hashed table,
// including the column index mapping (origIdx -> hashedIdx).
type MatchResult struct {
	OrigTable     string
	HashedTable   string
	ColumnMapping []int
	Strategy      int // which strategy matched (1, 2, or 3)
}

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

	os.Remove(generatedDBPath)

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

	originalTables := loadTables(originalDB, true)
	hashedTables := loadTables(hashedDB, false)

	hashedTableIndex := buildColumnCountIndex(hashedTables)

	newDB, err := sql.Open("sqlite3", generatedDBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer newDB.Close()

	// WAL mode for faster insertions
	_, err = newDB.Exec("PRAGMA journal_mode = WAL;")
	if err != nil {
		log.Fatal(err)
	}

	matchedByStrategy := map[int]int{1: 0, 2: 0, 3: 0}
	var unmatched []string
	usedHashedTables := map[string]bool{}

	for _, orig := range originalTables {
		if filter != "" {
			if _, ok := filterTables[orig.Name]; !ok {
				continue
			}
		}

		if orig.RowCount == 0 {
			copyEmptyTable(originalDB, newDB, orig.Name)
			log.Printf("[skip] %s (empty table, schema copied)", orig.Name)
			continue
		}

		// Pre-filter: candidates with same column count, not already used
		candidates := make([]*TableInfo, 0)
		for _, c := range hashedTableIndex[orig.ColumnCount] {
			if !usedHashedTables[c.Name] {
				candidates = append(candidates, c)
			}
		}

		if len(candidates) == 0 {
			unmatched = append(unmatched, orig.Name)
			log.Printf("[FAIL] %s: no candidates with %d columns", orig.Name, orig.ColumnCount)
			continue
		}

		result := matchTable(orig, candidates, originalDB, hashedDB)
		if result == nil {
			unmatched = append(unmatched, orig.Name)
			log.Printf("[FAIL] %s: no match found among %d candidates", orig.Name, len(candidates))
			continue
		}

		matchedByStrategy[result.Strategy]++
		usedHashedTables[result.HashedTable] = true
		tableMapping[result.OrigTable] = result.HashedTable

		log.Printf("[strategy %d] %s -> %s", result.Strategy, result.OrigTable, result.HashedTable)
		copyData(originalDB, hashedDB, newDB, result.OrigTable, result.HashedTable, result.ColumnMapping)
	}

	log.Printf("=== Summary ===")
	log.Printf("Matched: strategy1=%d, strategy2=%d, strategy3=%d, total=%d/%d",
		matchedByStrategy[1], matchedByStrategy[2], matchedByStrategy[3],
		matchedByStrategy[1]+matchedByStrategy[2]+matchedByStrategy[3],
		len(originalTables))
	if len(unmatched) > 0 {
		log.Printf("Unmatched (%d): %s", len(unmatched), strings.Join(unmatched, ", "))
	}

	if generateHashJson {
		writeJson()
	}

	log.Println("Done!")
}

// loadTables loads table metadata and first N rows from a database.
// If filterV1 is true, v1_ tables are excluded (used for the original DB).
func loadTables(db *sql.DB, filterV1 bool) []*TableInfo {
	tableNames := getTableNames(db, filterV1)
	tables := make([]*TableInfo, 0, len(tableNames))

	for _, name := range tableNames {
		colCount := getColumnCount(db, name)
		rowCount := countRowsInTable(db, name)
		firstRows := getFirstNRows(db, name, 10)

		tables = append(tables, &TableInfo{
			Name:        name,
			ColumnCount: colCount,
			RowCount:    rowCount,
			FirstNRows:  firstRows,
		})
	}

	return tables
}

// buildColumnCountIndex indexes tables by their column count for fast candidate lookup.
func buildColumnCountIndex(tables []*TableInfo) map[int][]*TableInfo {
	index := make(map[int][]*TableInfo)
	for _, t := range tables {
		index[t.ColumnCount] = append(index[t.ColumnCount], t)
	}
	return index
}

// matchTable runs the 3-strategy cascade to find the best match for an original table.
func matchTable(orig *TableInfo, candidates []*TableInfo, origDB, hashDB *sql.DB) *MatchResult {
	// Strategy 1: Sorted Row Comparison (N=3)
	matched := trySortedRowMatch(orig, candidates)
	if len(matched) == 1 {
		colMapping := inferColumnMapping(origDB, hashDB, orig.Name, matched[0].Name)
		return &MatchResult{
			OrigTable:     orig.Name,
			HashedTable:   matched[0].Name,
			ColumnMapping: colMapping,
			Strategy:      1,
		}
	}
	if len(matched) > 1 {
		// Disambiguate by closest row count
		best := disambiguateByRowCount(orig, matched)
		if best != nil {
			colMapping := inferColumnMapping(origDB, hashDB, orig.Name, best.Name)
			return &MatchResult{
				OrigTable:     orig.Name,
				HashedTable:   best.Name,
				ColumnMapping: colMapping,
				Strategy:      1,
			}
		}
	}

	// Strategy 2: Multiset Similarity (N=10)
	bestCandidate, bestScore := tryMultisetMatch(orig, candidates, hashDB)
	if bestCandidate != nil && bestScore >= 0.5 {
		colMapping := inferColumnMapping(origDB, hashDB, orig.Name, bestCandidate.Name)
		return &MatchResult{
			OrigTable:     orig.Name,
			HashedTable:   bestCandidate.Name,
			ColumnMapping: colMapping,
			Strategy:      2,
		}
	}

	// Strategy 3: Unique Value Fingerprinting
	fpMatch := tryFingerprintMatch(orig, candidates, origDB, hashDB)
	if fpMatch != nil {
		colMapping := inferColumnMapping(origDB, hashDB, orig.Name, fpMatch.Name)
		return &MatchResult{
			OrigTable:     orig.Name,
			HashedTable:   fpMatch.Name,
			ColumnMapping: colMapping,
			Strategy:      3,
		}
	}

	return nil
}

// trySortedRowMatch compares first 3 rows with values sorted within each row.
func trySortedRowMatch(orig *TableInfo, candidates []*TableInfo) []*TableInfo {
	n := 3
	origSorted := sortedRows(orig.FirstNRows, n)
	if len(origSorted) == 0 {
		return nil
	}

	var matched []*TableInfo
	for _, c := range candidates {
		candSorted := sortedRows(c.FirstNRows, n)
		if len(candSorted) != len(origSorted) {
			continue
		}
		allMatch := true
		for i := range origSorted {
			if !strSliceEqual(origSorted[i], candSorted[i]) {
				allMatch = false
				break
			}
		}
		if allMatch {
			matched = append(matched, c)
		}
	}
	return matched
}

// sortedRows takes up to n rows and returns each row with values sorted.
func sortedRows(rows [][]string, n int) [][]string {
	limit := n
	if len(rows) < limit {
		limit = len(rows)
	}
	result := make([][]string, limit)
	for i := 0; i < limit; i++ {
		sorted := make([]string, len(rows[i]))
		copy(sorted, rows[i])
		sort.Strings(sorted)
		result[i] = sorted
	}
	return result
}

// disambiguateByRowCount picks the candidate with the closest row count.
func disambiguateByRowCount(orig *TableInfo, candidates []*TableInfo) *TableInfo {
	if len(candidates) == 0 {
		return nil
	}
	best := candidates[0]
	bestDiff := int(math.Abs(float64(orig.RowCount - best.RowCount)))
	for _, c := range candidates[1:] {
		diff := int(math.Abs(float64(orig.RowCount - c.RowCount)))
		if diff < bestDiff {
			best = c
			bestDiff = diff
		}
	}
	return best
}

// tryMultisetMatch computes Jaccard similarity on value frequency maps.
func tryMultisetMatch(orig *TableInfo, candidates []*TableInfo, hashDB *sql.DB) (*TableInfo, float64) {
	origMultiset := buildMultiset(orig.FirstNRows, 10)

	var bestCandidate *TableInfo
	bestScore := 0.0

	for _, c := range candidates {
		candMultiset := buildMultiset(c.FirstNRows, 10)
		score := jaccardSimilarity(origMultiset, candMultiset)
		if score > bestScore {
			bestScore = score
			bestCandidate = c
		}
	}

	return bestCandidate, bestScore
}

// buildMultiset creates a value frequency map from up to n rows.
func buildMultiset(rows [][]string, n int) map[string]int {
	freq := make(map[string]int)
	limit := n
	if len(rows) < limit {
		limit = len(rows)
	}
	for i := 0; i < limit; i++ {
		for _, v := range rows[i] {
			freq[v]++
		}
	}
	return freq
}

// jaccardSimilarity computes Jaccard similarity between two multisets.
func jaccardSimilarity(a, b map[string]int) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 1.0
	}

	intersection := 0
	union := 0

	allKeys := make(map[string]bool)
	for k := range a {
		allKeys[k] = true
	}
	for k := range b {
		allKeys[k] = true
	}

	for k := range allKeys {
		av := a[k]
		bv := b[k]
		if av < bv {
			intersection += av
			union += bv
		} else {
			intersection += bv
			union += av
		}
	}

	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

// tryFingerprintMatch uses distinctive values from the original table to find matches.
func tryFingerprintMatch(orig *TableInfo, candidates []*TableInfo, origDB, hashDB *sql.DB) *TableInfo {
	// Fetch up to 50 rows from original
	origRows := getFirstNRows(origDB, orig.Name, 50)

	// Collect distinctive values (length > 4)
	fingerprints := make(map[string]bool)
	for _, row := range origRows {
		for _, v := range row {
			if len(v) > 4 {
				fingerprints[v] = true
			}
		}
	}

	if len(fingerprints) == 0 {
		return nil
	}

	var bestCandidate *TableInfo
	bestRatio := 0.0

	for _, c := range candidates {
		candRows := getFirstNRows(hashDB, c.Name, 200)
		candValues := make(map[string]bool)
		for _, row := range candRows {
			for _, v := range row {
				candValues[v] = true
			}
		}

		matchCount := 0
		for fp := range fingerprints {
			if candValues[fp] {
				matchCount++
			}
		}

		ratio := float64(matchCount) / float64(len(fingerprints))
		if ratio > bestRatio {
			bestRatio = ratio
			bestCandidate = c
		}
	}

	if bestRatio > 0.1 {
		return bestCandidate
	}
	return nil
}

// inferColumnMapping determines which hashed column maps to which original column.
// Returns a slice where result[origIdx] = hashedIdx.
func inferColumnMapping(origDB, hashDB *sql.DB, origTable, hashTable string) []int {
	n := 200
	origRows := getFirstNRows(origDB, origTable, n)
	hashRows := getFirstNRows(hashDB, hashTable, n)

	origColCount := getColumnCount(origDB, origTable)
	hashColCount := getColumnCount(hashDB, hashTable)

	if origColCount != hashColCount {
		// Fallback: identity mapping (shouldn't happen after column count pre-filter)
		mapping := make([]int, origColCount)
		for i := range mapping {
			mapping[i] = i
		}
		return mapping
	}

	colCount := origColCount
	mapping := make([]int, colCount)
	for i := range mapping {
		mapping[i] = -1
	}

	usedHash := make([]bool, colCount)

	// Build column value vectors
	origCols := buildColumnVectors(origRows, colCount)
	hashCols := buildColumnVectors(hashRows, colCount)

	// Exact match on value vectors
	for i := 0; i < colCount; i++ {
		if mapping[i] != -1 {
			continue
		}
		for j := 0; j < colCount; j++ {
			if usedHash[j] {
				continue
			}
			if strSliceEqual(origCols[i], hashCols[j]) {
				mapping[i] = j
				usedHash[j] = true
				break
			}
		}
	}

	// Statistical match using Jaccard on value frequency maps
	for i := 0; i < colCount; i++ {
		if mapping[i] != -1 {
			continue
		}
		origFreq := buildFreqMap(origCols[i])
		bestJ := -1
		bestSim := 0.3 // minimum threshold

		for j := 0; j < colCount; j++ {
			if usedHash[j] {
				continue
			}
			hashFreq := buildFreqMap(hashCols[j])
			sim := jaccardSimilarity(origFreq, hashFreq)
			if sim > bestSim {
				bestSim = sim
				bestJ = j
			}
		}

		if bestJ >= 0 {
			mapping[i] = bestJ
			usedHash[bestJ] = true
		}
	}

	// Elimination — if exactly 1 unmapped remains on each side
	unmappedOrig := []int{}
	unmappedHash := []int{}
	for i := 0; i < colCount; i++ {
		if mapping[i] == -1 {
			unmappedOrig = append(unmappedOrig, i)
		}
	}
	for j := 0; j < colCount; j++ {
		if !usedHash[j] {
			unmappedHash = append(unmappedHash, j)
		}
	}
	if len(unmappedOrig) == len(unmappedHash) {
		for k := range unmappedOrig {
			mapping[unmappedOrig[k]] = unmappedHash[k]
		}
	}

	// Final fallback: any still unmapped get identity mapping
	for i := range mapping {
		if mapping[i] == -1 {
			mapping[i] = i
		}
	}

	return mapping
}

// buildColumnVectors transposes row data into per-column value slices.
func buildColumnVectors(rows [][]string, colCount int) [][]string {
	cols := make([][]string, colCount)
	for i := range cols {
		cols[i] = make([]string, 0, len(rows))
	}
	for _, row := range rows {
		for j := 0; j < colCount && j < len(row); j++ {
			cols[j] = append(cols[j], row[j])
		}
	}
	return cols
}

// buildFreqMap creates a frequency map from a slice of strings.
func buildFreqMap(values []string) map[string]int {
	freq := make(map[string]int)
	for _, v := range values {
		freq[v]++
	}
	return freq
}

func copyData(originalDB, hashedDB, newDB *sql.DB, origTable, hashedTable string, colMapping []int) {
	createStmt, err := getCreateTableStatement(originalDB, origTable)
	if err != nil {
		log.Fatalf("Error getting CREATE TABLE statement for table %s: %v", origTable, err)
	}

	_, err = newDB.Exec(createStmt)
	if err != nil {
		log.Fatalf("Error creating table %s in new database: %v", origTable, err)
	}

	hashCols := getColumnNames(hashedDB, hashedTable)
	selectCols := make([]string, len(colMapping))
	for origIdx, hashIdx := range colMapping {
		if hashIdx >= 0 && hashIdx < len(hashCols) {
			selectCols[origIdx] = fmt.Sprintf("[%s]", hashCols[hashIdx])
		} else {
			selectCols[origIdx] = fmt.Sprintf("[%s]", hashCols[origIdx])
		}
	}
	query := fmt.Sprintf("SELECT %s FROM [%s]", strings.Join(selectCols, ", "), hashedTable)

	rows, err := hashedDB.Query(query)
	if err != nil {
		log.Fatalf("Error querying hashed table %s: %v", hashedTable, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns from hashed table %s: %v", hashedTable, err)
	}
	colCount := len(cols)

	placeholders := make([]string, colCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertStmt := fmt.Sprintf("INSERT INTO [%s] VALUES (%s)", origTable, strings.Join(placeholders, ", "))

	tx, err := newDB.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(insertStmt)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Error preparing insert statement for %s: %v", origTable, err)
	}
	defer stmt.Close()

	for rows.Next() {
		values := make([]interface{}, colCount)
		valuePtrs := make([]interface{}, colCount)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			tx.Rollback()
			log.Fatalf("Error scanning row from hashed table %s: %v", hashedTable, err)
		}

		_, err := stmt.Exec(values...)
		if err != nil {
			tx.Rollback()
			log.Fatalf("Error inserting data into table %s: %v", origTable, err)
		}
	}

	if err = tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

func copyEmptyTable(originalDB, newDB *sql.DB, origTable string) {
	createStmt, err := getCreateTableStatement(originalDB, origTable)
	if err != nil {
		log.Fatalf("Error getting CREATE TABLE statement for table %s: %v", origTable, err)
	}
	_, err = newDB.Exec(createStmt)
	if err != nil {
		log.Fatalf("Error creating empty table %s in new database: %v", origTable, err)
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
		if name == "sqlite_stat1" {
			continue
		}
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

func getColumnCount(db *sql.DB, tableName string) int {
	query := fmt.Sprintf("PRAGMA table_info([%s])", tableName)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error getting column count for table %s: %v", tableName, err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	return count
}

func getColumnNames(db *sql.DB, tableName string) []string {
	query := fmt.Sprintf("PRAGMA table_info([%s])", tableName)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error getting column names for table %s: %v", tableName, err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var dfltValue *string
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			log.Fatalf("Error scanning PRAGMA table_info for %s: %v", tableName, err)
		}
		names = append(names, name)
	}
	return names
}

func getFirstNRows(db *sql.DB, tableName string, n int) [][]string {
	query := fmt.Sprintf("SELECT * FROM [%s] LIMIT %d", tableName, n)
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

		rowValues := make([]string, len(cols))
		for i, col := range columns {
			rowValues[i] = fmt.Sprintf("%v", col)
		}
		tableData = append(tableData, rowValues)
	}
	return tableData
}

func countRowsInTable(db *sql.DB, tableName string) int {
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)).Scan(&count)
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

func strSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
