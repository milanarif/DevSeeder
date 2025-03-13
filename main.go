package main

import (
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	cfg := interactiveConfig()

	prodDB, devDB, err := OpenDatabases(cfg)
	if err != nil {
		log.Fatalf("Error opening databases: %v\n", err)
	}

	// Close connections once all operations are completed.
	defer prodDB.Close()
	defer devDB.Close()

	// By setting foreign_key_checks to 0, we can disable foreign key constraints during data synchronization.
	// This allows us to perform operations that would otherwise violate foreign key constraints.
	if _, err := devDB.Exec("SET foreign_key_checks = 0"); err != nil {
		log.Printf("Warning: cannot disable foreign_key_checks: %v\n", err)
	}

	// Fetch all foreign keys from the production database.
	allFks, err := FetchAllForeignKeys(prodDB) // from fks.go
	if err != nil {
		log.Fatalf("Error fetching all FKs: %v\n", err)
	}

	SyncPartialData(prodDB, devDB, allFks, cfg.Tables, cfg.ResetTables)

	if _, err := devDB.Exec("SET foreign_key_checks = 1"); err != nil {
		log.Printf("Warning: cannot re-enable foreign_key_checks: %v\n", err)
	}
}
