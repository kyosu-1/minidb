package main

import (
	"bufio"
	"flag"
	"fmt"
	"minidb/internal/engine"
	"minidb/internal/sql"
	"minidb/pkg/types"
	"os"
	"strings"
)

const banner = `
 __  __ _       _ ____  ____  
|  \/  (_)_ __ (_)  _ \| __ ) 
| |\/| | | '_ \| | | | |  _ \ 
| |  | | | | | | | |_| | |_) |
|_|  |_|_|_| |_|_|____/|____/ 
                              
A disk-based database with WAL, MVCC, B-Tree, and transactions
Type 'help' for available commands, 'exit' to quit.
`

func main() {
	dataDir := flag.String("data", "./minidb-data", "Data directory")
	bufferSize := flag.Int("buffer", 1024, "Buffer pool size (pages)")
	flag.Parse()

	fmt.Print(banner)

	// Initialize engine
	fmt.Printf("Data directory: %s\n", *dataDir)
	fmt.Printf("Buffer pool: %d pages (%d KB)\n", *bufferSize, *bufferSize*4)

	db, err := engine.New(engine.Config{
		DataDir:        *dataDir,
		BufferPoolSize: *bufferSize,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("Database ready.")
	fmt.Println()

	// Start REPL
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("minidb> ")

		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Handle special commands
		lower := strings.ToLower(input)
		switch {
		case lower == "exit" || lower == "quit" || lower == "\\q":
			fmt.Println("Goodbye!")
			return
		case lower == "help" || lower == "\\h":
			printHelp()
			continue
		case lower == "stats" || lower == "\\s":
			printStats(db)
			continue
		case lower == "checkpoint":
			if err := db.Checkpoint(); err != nil {
				fmt.Printf("Checkpoint failed: %v\n", err)
			} else {
				fmt.Println("Checkpoint created.")
			}
			continue
		case lower == "vacuum":
			vacuumDB(db)
			continue
		case lower == "tables" || lower == "\\dt":
			printTables(db)
			continue
		case strings.HasPrefix(lower, "create index on "):
			rest := strings.TrimPrefix(lower, "create index on ")
			rest = strings.TrimSpace(rest)
			// Parse: table(column)
			parenIdx := strings.Index(rest, "(")
			if parenIdx < 0 || !strings.HasSuffix(rest, ")") {
				fmt.Println("Usage: create index on <table>(<column>)")
				continue
			}
			tableName := strings.TrimSpace(rest[:parenIdx])
			columnName := strings.TrimSpace(rest[parenIdx+1 : len(rest)-1])
			if err := db.CreateIndex(tableName, columnName); err != nil {
				fmt.Printf("Create index failed: %v\n", err)
			} else {
				fmt.Printf("Index created on %s(%s)\n", tableName, columnName)
			}
			continue
		}

		// Execute SQL
		result := db.Execute(input)
		printResult(result)
	}
}

func printHelp() {
	help := `
Commands:
  help, \h          Show this help message
  stats, \s         Show database statistics
  tables, \dt       List all tables
  checkpoint        Create a checkpoint
  vacuum            Remove dead tuples (MVCC garbage collection)
  create index on <table>(<column>)  Create B-Tree index
  exit, quit        Exit the database

SQL Statements:
  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
    Types: INT, TEXT, BOOL
    
  INSERT INTO table (col1, col2) VALUES (val1, val2)
  
  SELECT col1, col2 FROM table [WHERE condition]
  SELECT * FROM table
  
  UPDATE table SET col1 = val1 [WHERE condition]
  
  DELETE FROM table [WHERE condition]
  
  BEGIN       Start a transaction
  COMMIT      Commit the current transaction
  ROLLBACK    Rollback the current transaction

Storage Architecture:
  ┌─────────────────────────────────────────┐
  │            Buffer Pool (LRU)            │
  │  Page1 ←→ Page2 ←→ Page3 ←→ ...        │
  └─────────────────────┬───────────────────┘
                        │ read/write
  ┌─────────────────────┼───────────────────┐
  │               Disk Files                │
  │  ┌──────────┐  ┌──────────┐            │
  │  │ data.db  │  │ wal.log  │            │
  │  │ (pages)  │  │ (logs)   │            │
  │  └──────────┘  └──────────┘            │
  └─────────────────────────────────────────┘

Examples:
  CREATE TABLE users (id INT, name TEXT, active BOOL)
  INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)
  SELECT * FROM users
  checkpoint
  stats
`
	fmt.Println(help)
}

func vacuumDB(db *engine.Engine) {
	result, err := db.Vacuum()
	if err != nil {
		fmt.Printf("VACUUM failed: %v\n", err)
		return
	}
	total := result.TotalRemoved()
	if total == 0 {
		fmt.Println("VACUUM: removed 0 dead tuples.")
	} else {
		fmt.Printf("VACUUM: removed %d dead tuples.\n", total)
		for _, ts := range result.Tables {
			if ts.TuplesRemoved > 0 {
				fmt.Printf("  %s: scanned %d, removed %d\n", ts.TableName, ts.TuplesScanned, ts.TuplesRemoved)
			}
		}
	}
}

func printStats(db *engine.Engine) {
	stats := db.Stats()
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║         Database Statistics              ║")
	fmt.Println("╠══════════════════════════════════════════╣")
	fmt.Printf("║  WAL Current LSN:    %-19v ║\n", stats["wal_current_lsn"])
	fmt.Printf("║  WAL Flushed LSN:    %-19v ║\n", stats["wal_flushed_lsn"])
	fmt.Printf("║  Active Txns:        %-19v ║\n", stats["active_txns"])
	fmt.Println("╠══════════════════════════════════════════╣")
	fmt.Printf("║  Disk Pages:         %-19v ║\n", stats["disk_pages"])
	fmt.Printf("║  Tables:             %-19v ║\n", stats["tables"])
	fmt.Println("╠══════════════════════════════════════════╣")
	fmt.Printf("║  Buffer Pool Hits:   %-19v ║\n", stats["buffer_pool_hits"])
	fmt.Printf("║  Buffer Pool Misses: %-19v ║\n", stats["buffer_pool_misses"])
	fmt.Printf("║  Buffer Pool Cached: %-19v ║\n", stats["buffer_pool_cached"])
	fmt.Printf("║  Buffer Hit Rate:    %-19v ║\n", stats["buffer_hit_rate"])
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()
}

func printTables(db *engine.Engine) {
	catalog := db.GetCatalog()
	tables := catalog.GetAllTables()

	if len(tables) == 0 {
		fmt.Println("No tables found.")
		return
	}

	fmt.Println("\nTables:")
	for _, name := range tables {
		schema := catalog.GetSchema(name)
		tableID, _ := catalog.GetTableID(name)
		fmt.Printf("  %s (id=%d)\n", name, tableID)
		for _, col := range schema.Columns {
			nullable := ""
			if !col.Nullable {
				nullable = " NOT NULL"
			}
			typeName := "UNKNOWN"
			switch col.Type {
			case types.ValueTypeInt:
				typeName = "INT"
			case types.ValueTypeString:
				typeName = "TEXT"
			case types.ValueTypeBool:
				typeName = "BOOL"
			}
			fmt.Printf("    - %s %s%s\n", col.Name, typeName, nullable)
		}
	}
	fmt.Println()
}

func printResult(result *sql.Result) {
	if result.Error != nil {
		fmt.Printf("ERROR: %v\n", result.Error)
		return
	}

	if len(result.Rows) > 0 {
		// Print table header
		widths := make([]int, len(result.Columns))
		for i, col := range result.Columns {
			widths[i] = len(col)
		}

		// Calculate column widths
		for _, row := range result.Rows {
			for i, val := range row.Values {
				strVal := formatValue(val)
				if len(strVal) > widths[i] {
					widths[i] = len(strVal)
				}
			}
		}

		// Print header
		printSeparator(widths)
		printRow(result.Columns, widths)
		printSeparator(widths)

		// Print rows
		for _, row := range result.Rows {
			vals := make([]string, len(row.Values))
			for i, val := range row.Values {
				vals[i] = formatValue(val)
			}
			printRow(vals, widths)
		}
		printSeparator(widths)

		fmt.Println()
	}

	if result.Message != "" {
		fmt.Println(result.Message)
	}
}

func formatValue(val types.Value) string {
	if val.IsNull {
		return "NULL"
	}
	switch val.Type {
	case types.ValueTypeInt:
		return fmt.Sprintf("%d", val.IntVal)
	case types.ValueTypeString:
		return val.StrVal
	case types.ValueTypeBool:
		if val.BoolVal {
			return "true"
		}
		return "false"
	default:
		return "NULL"
	}
}

func printRow(values []string, widths []int) {
	fmt.Print("│ ")
	for i, val := range values {
		fmt.Printf("%-*s │ ", widths[i], val)
	}
	fmt.Println()
}

func printSeparator(widths []int) {
	fmt.Print("├")
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print("┼")
		}
	}
	fmt.Println("┤")
}
