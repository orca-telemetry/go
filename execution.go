package orca

import (
	"fmt"
	pb "github.com/orca-telemetry/core/protobufs/go"
)

// DependencyResultRow represents a single result from a dependency
type DependencyResultRow struct {
	Window Window
	// FIXME: Use type constraints
	Result any // float64, []float64, or map[string]interface{}
}

// DependencyAlgorithm represents metadata about a dependency algorithm
type DependencyAlgorithm struct {
	Name        string
	Version     string
	Description string
}

// FullName returns the full algorithm name as "name_version"
func (d DependencyAlgorithm) FullName() string {
	return fmt.Sprintf("%s_%s", d.Name, d.Version)
}

// ID returns the algorithm ID (same as FullName for dependencies)
func (d DependencyAlgorithm) ID() string {
	return d.FullName()
}

// DependencyResult holds results from a dependency algorithm
type DependencyResult struct {
	Algorithm DependencyAlgorithm
	Results   []DependencyResultRow
}

// Dependencies manages algorithm dependencies
type Dependencies struct {
	deps map[string]*DependencyResult
}

// NewDependencies creates a new Dependencies instance
func NewDependencies() *Dependencies {
	return &Dependencies{
		deps: make(map[string]*DependencyResult),
	}
}

// GetResult retrieves a dependency result by algorithm function
func (d *Dependencies) GetResult(algoFunc AlgorithmFunc) *DependencyResult {
	if d.deps == nil {
		return nil
	}
	name, version := extractAlgorithmMeta(algoFunc)
	if name == "" || version == "" {
		return nil
	}
	fullName := fmt.Sprintf("%s_%s", name, version)
	return d.deps[fullName]
}

// ExecutionParams provides context for algorithm execution
type ExecutionParams struct {
	Window       Window
	Dependencies *Dependencies
}

// NewExecutionParams creates ExecutionParams from a protobuf Window
func NewExecutionParams(window *pb.Window, deps *Dependencies) *ExecutionParams {
	metadata := make(map[string]any)
	if window.Metadata != nil {
		metadata = window.Metadata.AsMap()
	}

	return &ExecutionParams{
		Window: Window{
			TimeFrom: window.TimeFrom.AsTime(),
			TimeTo:   window.TimeTo.AsTime(),
			Name:     window.WindowTypeName,
			Version:  window.WindowTypeVersion,
			Origin:   window.Origin,
			Metadata: metadata,
		},
		Dependencies: deps,
	}
}
