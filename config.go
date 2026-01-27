package orca

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var (
	// connectionStringPattern matches "address:port" format
	connectionStringPattern = regexp.MustCompile(`^(.+):(\d+)$`)
)

// ConfigError represents configuration-related errors
type ConfigError struct {
	Type    string // "BadEnvVar", "BadConfigFile", "MissingEnvVar"
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

// NewBadEnvVar creates a new bad environment variable error
func NewBadEnvVar(msg string) error {
	return &ConfigError{Type: "BadEnvVar", Message: msg}
}

// NewBadConfigFile creates a new bad config file error
func NewBadConfigFile(msg string) error {
	return &ConfigError{Type: "BadConfigFile", Message: msg}
}

// NewMissingEnvVar creates a new missing environment variable error
func NewMissingEnvVar(msg string) error {
	return &ConfigError{Type: "MissingEnvVar", Message: msg}
}

// parseConnectionString parses a connection string of the form "address:port"
func parseConnectionString(connStr string) (address string, port int, err error) {
	// Check for leading/trailing whitespace
	if connStr != strings.TrimSpace(connStr) {
		return "", 0, fmt.Errorf("connection string has leading/trailing whitespace")
	}

	// Match the pattern
	matches := connectionStringPattern.FindStringSubmatch(connStr)
	if matches == nil {
		return "", 0, fmt.Errorf("invalid format, expected 'address:port'")
	}

	address = matches[1]
	if address == "" {
		return "", 0, fmt.Errorf("address cannot be empty")
	}

	port, err = strconv.Atoi(matches[2])
	if err != nil {
		return "", 0, fmt.Errorf("invalid port number: %w", err)
	}

	return address, port, nil
}

// configFileData represents the structure of orca.json
type configFileData struct {
	ProjectName               string `json:"projectName"`
	OrcaConnectionString      string `json:"orcaConnectionString"`
	ProcessorPort             int    `json:"processorPort"`
	ProcessorConnectionString string `json:"processorConnectionString"`
}

// Config holds the complete configuration for the Orca processor
type Config struct {
	IsProduction          bool
	ProjectName           string
	OrcaCore              string
	ProcessorHost         string
	ProcessorPort         int
	ProcessorExternalPort int
	HasConfigFile         bool
}

// parseConfigFile attempts to read and parse orca.json from the current directory
func parseConfigFile() (*configFileData, bool, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, false, fmt.Errorf("failed to get current directory: %w", err)
	}

	configPath := filepath.Join(cwd, "orca.json")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, false, nil
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, true, NewBadConfigFile(fmt.Sprintf("failed to read config file: %v", err))
	}

	// Parse JSON
	var config configFileData
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, true, NewBadConfigFile(fmt.Sprintf("failed to parse config file: %v", err))
	}

	// Validate processor connection string
	_, _, err = parseConnectionString(config.ProcessorConnectionString)
	if err != nil {
		return nil, true, NewBadConfigFile(
			fmt.Sprintf("processorConnectionString is not a valid address of the form <ip>:<port>: %v", err))
	}

	return &config, true, nil
}

// envConfig represents configuration from environment variables
type envConfig struct {
	isProduction          bool
	orcaCore              string
	processorAddress      string
	processorPort         *int
	processorExternalPort *int
}

// getEnvConfig reads configuration from environment variables
func getEnvConfig(strict bool) (*envConfig, error) {
	cfg := &envConfig{}

	// Parse ORCA_CORE
	orcaCore := os.Getenv("ORCA_CORE")
	if strict && orcaCore == "" {
		return nil, NewMissingEnvVar("ORCA_CORE is required")
	}
	// Strip grpc:// prefix if present
	cfg.orcaCore = strings.TrimPrefix(orcaCore, "grpc://")

	// Parse PROCESSOR_ADDRESS
	procAddress := os.Getenv("PROCESSOR_ADDRESS")
	if strict && procAddress == "" {
		return nil, NewMissingEnvVar("PROCESSOR_ADDRESS is required")
	}

	if procAddress != "" {
		address, port, err := parseConnectionString(procAddress)
		if err != nil {
			return nil, NewBadEnvVar(
				fmt.Sprintf("PROCESSOR_ADDRESS is not a valid address of the form <ip>:<port>: %v", err))
		}
		cfg.processorAddress = address
		cfg.processorPort = &port
	}

	// Parse PROCESSOR_EXTERNAL_PORT
	procExternalPortStr := os.Getenv("PROCESSOR_EXTERNAL_PORT")
	if procExternalPortStr != "" {
		port, err := strconv.Atoi(procExternalPortStr)
		if err != nil {
			return nil, NewBadEnvVar("PROCESSOR_EXTERNAL_PORT is not a valid number")
		}
		cfg.processorExternalPort = &port
	} else if strict {
		// In strict mode, if PROCESSOR_EXTERNAL_PORT is not set, use PROCESSOR_PORT
		if cfg.processorPort != nil {
			cfg.processorExternalPort = cfg.processorPort
		}
	}

	// Parse ENV
	env := os.Getenv("ENV")
	cfg.isProduction = env == "production"

	return cfg, nil
}

// LoadConfig loads configuration from both config file and environment variables.
// Config file takes priority, but environment variables can override.
// If no config file is present, all required environment variables must be set.
func LoadConfig() (*Config, error) {
	config := &Config{}

	// Try to load config file
	fileConfig, hasConfig, err := parseConfigFile()
	if err != nil {
		return nil, err
	}

	config.HasConfigFile = hasConfig

	if hasConfig {
		// Config file exists - use it as base
		config.ProjectName = fileConfig.ProjectName
		config.OrcaCore = fileConfig.OrcaConnectionString

		// Parse processor connection string from config file
		address, port, err := parseConnectionString(fileConfig.ProcessorConnectionString)
		if err != nil {
			return nil, NewBadConfigFile(fmt.Sprintf("invalid processorConnectionString: %v", err))
		}
		config.ProcessorHost = address
		config.ProcessorPort = port
		config.ProcessorExternalPort = fileConfig.ProcessorPort

		// Warn if project name is missing
		if config.ProjectName == "" {
			fmt.Fprintln(os.Stderr,
				"Warning: Project name could not be found in `orca.json` (or the config is not present). "+
					"When generating stubs with `orca sync` this may cause algorithm definitions that are present "+
					"in this repository to be duplicated locally.\n"+
					"Run `orca init` to generate a `orca.json` config file to avoid this.")
		}

		// Load env config (non-strict) to allow overrides
		envCfg, err := getEnvConfig(false)
		if err != nil {
			return nil, err
		}

		// Environment variables can override config file
		if envCfg.orcaCore != "" {
			config.OrcaCore = envCfg.orcaCore
		}
		if envCfg.processorAddress != "" {
			config.ProcessorHost = envCfg.processorAddress
		}
		if envCfg.processorPort != nil {
			config.ProcessorPort = *envCfg.processorPort
		}
		if envCfg.processorExternalPort != nil {
			config.ProcessorExternalPort = *envCfg.processorExternalPort
		}
		config.IsProduction = envCfg.isProduction

	} else {
		// No config file - all required env vars must be present
		envCfg, err := getEnvConfig(true)
		if err != nil {
			return nil, err
		}

		if envCfg.processorPort == nil {
			return nil, NewMissingEnvVar("PROCESSOR_PORT required")
		}
		if envCfg.processorExternalPort == nil {
			return nil, NewMissingEnvVar("PROCESSOR_EXTERNAL_PORT required")
		}

		config.IsProduction = envCfg.isProduction
		config.OrcaCore = envCfg.orcaCore
		config.ProcessorHost = envCfg.processorAddress
		config.ProcessorPort = *envCfg.processorPort
		config.ProcessorExternalPort = *envCfg.processorExternalPort
		config.ProjectName = "" // No config file means no project name
	}

	return config, nil
}

// MustLoadConfig loads configuration and panics on error.
// Useful for initialization in main().
func MustLoadConfig() *Config {
	config, err := LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load configuration: %v", err))
	}
	return config
}

// DefaultConfig returns a configuration with sensible defaults for development
func DefaultConfig() *Config {
	return &Config{
		IsProduction:          false,
		ProjectName:           "",
		OrcaCore:              "localhost:50050",
		ProcessorHost:         "localhost",
		ProcessorPort:         50051,
		ProcessorExternalPort: 50051,
		HasConfigFile:         false,
	}
}
