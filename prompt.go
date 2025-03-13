package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/manifoldco/promptui"
)

func promptForValue(label, defaultVal string) string {
	prompt := promptui.Prompt{
		Label:   label,
		Default: defaultVal,
	}
	result, err := prompt.Run()
	if err != nil {
		log.Fatalf("Prompt failed for '%s': %v\n", label, err)
	}
	return result
}

func promptForSecret(label, defaultVal string) string {
	prompt := promptui.Prompt{
		Label:   label,
		Default: defaultVal,
		Mask:    '*',
	}
	result, err := prompt.Run()
	if err != nil {
		log.Fatalf("Prompt failed for '%s': %v\n", label, err)
	}
	return result
}

func promptForInt(label, defaultVal string) int {
	valStr := promptForValue(label, defaultVal)
	valInt, err := strconv.Atoi(valStr)
	if err != nil {
		log.Fatalf("Invalid number for '%s': %v\n", label, err)
	}
	return valInt
}

func promptForBool(label string, defaultVal bool) bool {
	options := []string{"No", "Yes"}
	var defaultIndex = 0
	if defaultVal {
		defaultIndex = 1
	}

	prompt := promptui.Select{
		Label:     label,
		Items:     options,
		CursorPos: defaultIndex,
	}

	index, _, err := prompt.Run()
	if err != nil {
		log.Fatalf("Prompt failed for '%s': %v\n", label, err)
	}

	return index == 1
}

func parseTablesPrompt() map[string]int {
	tablesInput := promptForValue("Tables (format: table:limit,table:limit)", "events:1000,companies:1000")

	tables := make(map[string]int)
	pairs := strings.Split(tablesInput, ",")
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			log.Fatalf("Invalid table format '%s', expected table:limit", pair)
		}
		tableName := parts[0]
		limit, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Fatalf("Invalid limit for table '%s': %v", tableName, err)
		}
		tables[tableName] = limit
	}

	return tables
}

func buildDSN(user, pass, host string, port int, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, pass, host, port, dbName)
}

func interactiveConfig() *Config {
	fmt.Println("Configure Source Database (Prod) Connection:")

	prodUser := promptForValue("Prod DB User", "root")
	prodPass := promptForSecret("Prod DB Password", "")
	prodHost := promptForValue("Prod DB Host", "localhost")
	prodPort := promptForInt("Prod DB Port", "3306")
	prodDBName := promptForValue("Prod DB Name", "prod_db")

	prodDSN := buildDSN(prodUser, prodPass, prodHost, prodPort, prodDBName)

	fmt.Println("\nConfigure Target Database (Dev) Connection:")

	devUser := promptForValue("Dev DB User", "root")
	devPass := promptForSecret("Dev DB Password", "")
	devHost := promptForValue("Dev DB Host", "localhost")
	devPort := promptForInt("Dev DB Port", "3306")
	devDBName := promptForValue("Dev DB Name", "dev_db")

	devDSN := buildDSN(devUser, devPass, devHost, devPort, devDBName)

	fmt.Println("\nTables Configuration:")
	tables := parseTablesPrompt()

	disableFKChecks := promptForBool("Disable Foreign Key Checks?", false)
	resetTables := promptForBool("Reset Tables Before Sync?", true)

	return &Config{
		ProdDSN:         prodDSN,
		DevDSN:          devDSN,
		Tables:          tables,
		DisableFKChecks: disableFKChecks,
		ResetTables:     resetTables,
	}
}
