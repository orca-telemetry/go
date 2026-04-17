package orca

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"time"

	contract "github.com/orca-telemetry/contract/go"
)

// naming convention
var algorithmNamePattern = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)

// RemoteAlgorithm represents an algorithm from another processor
type RemoteAlgorithm struct {
	ProcessorName    string
	ProcessorRuntime string
	Name             string
	Version          string
}

// FullName returns the full name of the remote algorithm
func (r RemoteAlgorithm) FullName() string {
	return fmt.Sprintf("%s_%s", r.Name, r.Version)
}

// AlgorithmFunc is the signature for the function that is executed
// when an algorithm is run
type AlgorithmFunc func(params *ExecutionParams) (Result, error)

// Algorithm represents a registered algorithm
type Algorithm struct {
	Name           string
	Version        string
	Description    string
	WindowType     *WindowType
	ExecFunc       AlgorithmFunc
	ResultType     contract.ResultType
	Dependencies   []Dependency
	SelfLookbackN  int
	SelfLookbackTd time.Duration
}

// FullName returns the name of the algorithm as "<name>_<version>"
func (a Algorithm) FullName() string {
	return fmt.Sprintf("%s_%s", a.Name, a.Version)
}

// ID returns the globally unique identifier
func (a Algorithm) ID(processor string, runtime string) string {
	hash := md5.Sum([]byte(runtime))
	return fmt.Sprintf("%s_%s_%x", a.Name, a.Version, hash)
}

// AlgorithmOptions define a set of options for configuring
// the AddAlgorithm call
type AlgorithmOptions struct {
	DependsOn      []Dependency
	SelfLookbackN  int
	SelfLookbackTd time.Duration
}

type AlgorithmOption func(*AlgorithmOptions)

type Dependency struct {
	Algorithm *Algorithm
	Lookback  LookbackOption
}

// WithDependencies registers a set of dependencies
func WithDependencies(dependencies []Dependency) AlgorithmOption {
	return func(o *AlgorithmOptions) {
		o.DependsOn = dependencies
	}
}

// WithLookbackN sets the number-based lookback
func WithSelfLookbackN(n int) AlgorithmOption {
	return func(o *AlgorithmOptions) {
		o.SelfLookbackN = n
	}
}

// WithLookbackTD sets the time-based lookback
func WithSelfLookbackTD(td time.Duration) AlgorithmOption {
	return func(o *AlgorithmOptions) {
		o.SelfLookbackTd = td
	}
}

// NewAlgorithm creates a new instance of an algorithm, enforcing
// certain checks
//
// Should be used to create a new algorithm
func NewAlgorithm(
	name string,
	version string,
	description string,
	windowType *WindowType,
	execFunc AlgorithmFunc,
	resultKind ResultKind,
	options ...AlgorithmOption,
) (*Algorithm, error) {
	if !algorithmNamePattern.MatchString(name) {
		return nil, InvalidAlgorithmArgumentError{fmt.Sprintf("algorithm name '%s' must be in PascalCase", name)}
	}
	if !semverPattern.MatchString(version) {
		return nil, InvalidAlgorithmArgumentError{fmt.Sprintf("version '%s' must follow basic semantic versioning", version)}
	}

	opts := &AlgorithmOptions{}
	for _, option := range options {
		option(opts)
	}

	var resultTypeContract contract.ResultType
	switch resultKind {
	case KindStruct:
		resultTypeContract = contract.ResultType_STRUCT
	case KindArray:
		resultTypeContract = contract.ResultType_ARRAY
	case KindValue:
		resultTypeContract = contract.ResultType_VALUE
	case KindNone:
		resultTypeContract = contract.ResultType_NONE
	default:
		return nil, InvalidAlgorithmReturnTypeError{fmt.Sprintf("result type not supported. found: %v", resultKind)}
	}

	algo := &Algorithm{
		Name:           name,
		Version:        version,
		Description:    description,
		WindowType:     windowType,
		ExecFunc:       execFunc,
		ResultType:     resultTypeContract,
		Dependencies:   opts.DependsOn,
		SelfLookbackN:  opts.SelfLookbackN,
		SelfLookbackTd: opts.SelfLookbackTd,
	}

	return algo, nil
}
