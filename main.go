// Package Orca provides the Go SDK for integrating with the Orca stack
// to register, execute, and manage algorithms
package orca

import (
	"os"
	"reflect"
	"regexp"
	"time"

	"github.com/rs/zerolog"
)

var (
	// regex patterns for validation
	semverPattern = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`)
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func extractRemoteAlgorithmMeta(fn AlgorithmFunc) *RemoteAlgorithm {
	// Placeholder for remote algorithm metadata extraction
	return nil
}

func extractLookback(fn AlgorithmFunc) lookbackParams {
	// Placeholder for lookback extraction
	return lookbackParams{N: 0, TD: 0}
}

func isRemoteAlgorithm(fn AlgorithmFunc) bool {
	// Placeholder for remote algorithm detection
	return false
}

func isSameFunc(f1, f2 AlgorithmFunc) bool {
	return reflect.ValueOf(f1).Pointer() == reflect.ValueOf(f2).Pointer()
}

// Lookback adds lookback metadata to an algorithm function
type LookbackOption func(*lookbackParams)

// WithLookbackN sets the number-based lookback
func WithLookbackN(n int) LookbackOption {
	return func(p *lookbackParams) {
		p.N = n
	}
}

// WithLookbackTD sets the time-based lookback
func WithLookbackTD(td time.Duration) LookbackOption {
	return func(p *lookbackParams) {
		p.TD = td
	}
}

// WithLookbackGapN sets the number-based lookback
func WithLookbackGapN(n int) LookbackOption {
	return func(p *lookbackParams) {
		p.GapN = n
	}
}

// WithLookbackGapTD sets the time-based lookback
func WithLookbackGapTD(td time.Duration) LookbackOption {
	return func(p *lookbackParams) {
		p.GapTD = td
	}
}
