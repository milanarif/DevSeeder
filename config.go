package main

import (
	"database/sql"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration loaded from config.yaml
type Config struct {
	ProdDSN         string         `yaml:"prod_dsn"`
	DevDSN          string         `yaml:"dev_dsn"`
	Tables          map[string]int `yaml:"tables"`
	RootTable       string         `yaml:"root_table"`
	RootLimit       int            `yaml:"root_limit"`
	DisableFKChecks bool           `yaml:"disable_fk_checks"`
	ResetTables     bool           `yaml:"reset_tables"`

	// Optionally define anonymization rules, logs, etc.
	Anonymize map[string]string `yaml:"anonymize"`
}

// LoadConfig reads a YAML file and unmarshals into Config
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// OpenDatabases opens connections to the prod and dev MySQL databases
func OpenDatabases(cfg *Config) (*sql.DB, *sql.DB, error) {
	prodDB, err := sql.Open("mysql", cfg.ProdDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("prodDB connect error: %w", err)
	}

	devDB, err := sql.Open("mysql", cfg.DevDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("devDB connect error: %w", err)
	}

	// Ping to ensure databases are up
	if err := prodDB.Ping(); err != nil {
		return nil, nil, fmt.Errorf("prodDB ping error: %w", err)
	}
	if err := devDB.Ping(); err != nil {
		return nil, nil, fmt.Errorf("devDB ping error: %w", err)
	}

	return prodDB, devDB, nil
}
