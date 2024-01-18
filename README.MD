## PCR Hash Table Rename Tool

a tool to rename hashed tables to their original names

### Compile

#### gcc is required!

```bash
env CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC=x86_64-linux-musl-gcc go build -o pcr_hash_rename_tool_linux_amd64
env CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -o pcr_hash_rename_tool_darwin_arm64
env CGO_ENABLED=1 GOOS=windows GOARCH=amd64 CC=x86_64-w64-mingw32-gcc go build -o pcr_hash_rename_tool_windows_amd64.exe
```

### Usage

```
Usage:
  pcr-hash-table-rename [flags]

Flags:
  -f, --filter string            OPTIONAL: Use a file to generate a new database with only the tables in the file
  -t, --generateTableMapping     OPTIONAL: Generate a mapping of raw table name -> hash table name in JSON
  -g, --generatedDBPath string   OPTIONAL: Path to the new database, default to jp_fixed.db (default "jp_fixed.db")
  -n, --hashedDBPath string      REQUIRED: Path to the hashed (latest) database
  -h, --help                     help for pcr-hash-table-rename
  -r, --originalDBPath string    REQUIRED: Path to the original (human-readable one) database
```

### Example

```bash
./pcr_hash_rename_tool_darwin_arm64 --originalDBPath="absolute_path_to_raw_db" --hashedDBPath="absolute_path_to_hashed_db" --generatedDBPath="jp_fixed.db"
```