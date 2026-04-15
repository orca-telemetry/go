// Package Orca provides the Go SDK for integrating with the Orca stack
// to register, execute, and manage algorithms
package orca

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	contract "github.com/orca-telemetry/contract/go"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	// regex patterns for validation
	algorithmNamePattern = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)
	semverPattern        = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`)
	windowNamePattern    = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
}

// AlgorithmFunc is the signature for algorithm execution functions
type AlgorithmFunc func(params *ExecutionParams) (Result, error)

// RemoteAlgorithm represents an algorithm from another processor
type RemoteAlgorithm struct {
	ProcessorName    string
	ProcessorRuntime string
	Name             string
	Version          string
}

// FullName returns the full algorithm name
func (r RemoteAlgorithm) FullName() string {
	return fmt.Sprintf("%s_%s", r.Name, r.Version)
}

// Algorithm represents a registered algorithm
type Algorithm struct {
	Name        string
	Version     string
	Description string
	WindowType  WindowType
	ExecFunc    AlgorithmFunc
	Processor   string
	Runtime     string
	ResultType  contract.ResultType
	LookbackN   int
	LookbackTD  time.Duration
}

// FullName returns the full algorithm name as "name_version"
func (a Algorithm) FullName() string {
	return fmt.Sprintf("%s_%s", a.Name, a.Version)
}

// ID returns the globally unique identifier
func (a Algorithm) ID() string {
	hash := md5.Sum([]byte(a.Runtime))
	return fmt.Sprintf("%s_%s_%x", a.Name, a.Version, hash)
}

// FullWindowName returns the full window name
func (a Algorithm) FullWindowName() string {
	return a.WindowType.FullName()
}

// algorithmRegistry manages all registered algorithms
type algorithmRegistry struct {
	mu                 sync.RWMutex
	algorithms         map[string]*Algorithm
	dependencies       map[string][]*Algorithm
	dependencyFuncs    map[string][]AlgorithmFunc
	remoteDependencies map[string][]RemoteAlgorithm
	windowTriggers     map[string][]*Algorithm
	lookbacks          map[string]map[string]lookbackParams
}

type lookbackParams struct {
	N  int
	TD time.Duration
}

func newAlgorithmRegistry() *algorithmRegistry {
	return &algorithmRegistry{
		algorithms:         make(map[string]*Algorithm),
		dependencies:       make(map[string][]*Algorithm),
		dependencyFuncs:    make(map[string][]AlgorithmFunc),
		remoteDependencies: make(map[string][]RemoteAlgorithm),
		windowTriggers:     make(map[string][]*Algorithm),
		lookbacks:          make(map[string]map[string]lookbackParams),
	}
}

func (r *algorithmRegistry) addAlgorithm(name string, algo *Algorithm) error {
	r.mu.Lock()

	defer r.mu.Unlock()

	if _, exists := r.algorithms[name]; exists {
		return fmt.Errorf("algorithm %s already exists", name)
	}
	log.Info().Msg(fmt.Sprintf("Registering algorithm: %s (window: %s)", name, algo.FullWindowName()))

	r.algorithms[name] = algo

	return nil
}

func (r *algorithmRegistry) addDependency(algoName string, dep AlgorithmFunc, remote bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if remote {
		// handle remote dependency
		remoteMeta := extractRemoteAlgorithmMeta(dep)
		if remoteMeta == nil {
			return BrokenRemoteAlgorithmStubsError{"could not parse metadata from Orca stubs. Rerun stub generation: `orca sync`"}
		}

		r.remoteDependencies[algoName] = append(r.remoteDependencies[algoName], *remoteMeta)
		r.addLookbackLocked(algoName, remoteMeta.FullName(), extractLookback(dep))
		return nil
	}

	// Find the dependency algorithm
	var depAlgo *Algorithm
	for _, algo := range r.algorithms {
		if isSameFunc(algo.ExecFunc, dep) {
			depAlgo = algo
			break
		}
	}
	if depAlgo == nil {
		return fmt.Errorf("dependency not found")
	}

	r.dependencyFuncs[algoName] = append(r.dependencyFuncs[algoName], dep)
	r.dependencies[algoName] = append(r.dependencies[algoName], depAlgo)
	r.addLookbackLocked(algoName, depAlgo.FullName(), extractLookback(dep))

	return nil
}

func (r *algorithmRegistry) addWindowTrigger(windowName string, algo *Algorithm) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.windowTriggers[windowName] = append(r.windowTriggers[windowName], algo)
}

func (r *algorithmRegistry) hasAlgorithmFunc(fn AlgorithmFunc) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, algo := range r.algorithms {
		if isSameFunc(algo.ExecFunc, fn) {
			return true
		}
	}
	return false
}

func (r *algorithmRegistry) addLookbackLocked(from, to string, params lookbackParams) {
	if r.lookbacks[from] == nil {
		r.lookbacks[from] = make(map[string]lookbackParams)
	}
	r.lookbacks[from][to] = params
}

func (r *algorithmRegistry) getLookback(from, to string) lookbackParams {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if toMap, exists := r.lookbacks[from]; exists {
		if params, exists := toMap[to]; exists {
			return params
		}
	}
	return lookbackParams{N: 0, TD: 0}
}

// Processor is the entry point for registering algorithms in Orca
type Processor struct {
	contract.UnimplementedOrcaProcessorServer

	name                  string
	processorConnStr      string
	orcaProcessorConnStr  string
	runtime               string
	maxWorkers            int
	registry              *algorithmRegistry
	orcaCore              string
	isProduction          bool
	projectName           string
	processorPort         string
	processorHost         string
	processorExternalPort string
}

// ProcessorOption configures a Processor
type ProcessorOption func(*Processor)

// WithMaxWorkers sets the maximum number of concurrent workers
func WithMaxWorkers(n int) ProcessorOption {
	return func(p *Processor) {
		p.maxWorkers = n
	}
}

// WithOrcaCore sets the Orca Core connection string
func WithOrcaCore(addr string) ProcessorOption {
	return func(p *Processor) {
		p.orcaCore = addr
	}
}

// WithProduction enables production mode (TLS)
func WithProduction(enabled bool) ProcessorOption {
	return func(p *Processor) {
		p.isProduction = enabled
	}
}

// WithProjectName sets the project name
func WithProjectName(name string) ProcessorOption {
	return func(p *Processor) {
		p.projectName = name
	}
}

// WithProcessorPort sets the processor port
func WithProcessorPort(port string) ProcessorOption {
	return func(p *Processor) {
		p.processorPort = port
	}
}

// WithProcessorHost sets the processor host
func WithProcessorHost(host string) ProcessorOption {
	return func(p *Processor) {
		p.processorHost = host
	}
}

// WithProcessorExternalPort sets the processor external port
func WithProcessorExternalPort(port string) ProcessorOption {
	return func(p *Processor) {
		p.processorExternalPort = port
	}
}

// NewProcessor creates a new Orca processor
func NewProcessor(name string, opts ...ProcessorOption) *Processor {
	p := &Processor{
		name:                  name,
		runtime:               runtime.Version(),
		maxWorkers:            10,
		registry:              newAlgorithmRegistry(),
		orcaCore:              getEnv("ORCA_CORE", "localhost:50051"),
		isProduction:          getEnv("ORCA_ENV", "development") == "production",
		projectName:           getEnv("PROJECT_NAME", ""),
		processorPort:         getEnv("PROCESSOR_PORT", "50052"),
		processorHost:         getEnv("PROCESSOR_HOST", "localhost"),
		processorExternalPort: getEnv("PROCESSOR_EXTERNAL_PORT", "50052"),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.processorConnStr = fmt.Sprintf(":%s", p.processorPort)
	p.orcaProcessorConnStr = fmt.Sprintf("%s:%s", p.processorHost, p.processorExternalPort)

	return p
}

type AlgorithmOptions struct {
	DependsOn []AlgorithmFunc
}
type AlgorithmOption func(*AlgorithmOptions)

func WithDependencies(dependencies []AlgorithmFunc) AlgorithmOption {
	return func(o *AlgorithmOptions) {
		o.DependsOn = dependencies
	}
}

// Algorithm registers an algorithm with the processor
func (p *Processor) Algorithm(
	name string,
	version string,
	description string,
	windowType WindowType,
	execFunc AlgorithmFunc,
	options ...AlgorithmOption,
) error {
	if !algorithmNamePattern.MatchString(name) {
		return InvalidAlgorithmArgumentError{fmt.Sprintf("algorithm name '%s' must be in PascalCase", name)}
	}
	if !semverPattern.MatchString(version) {
		return InvalidAlgorithmArgumentError{fmt.Sprintf("version '%s' must follow basic semantic versioning", version)}
	}
	if err := windowType.Validate(); err != nil {
		return err
	}

	resultType := inferResultType(execFunc)
	if resultType == contract.ResultType_NOT_SPECIFIED {
		return InvalidAlgorithmReturnTypeError{"cannot infer result type from algorithm function"}
	}

	opts := &AlgorithmOptions{DependsOn: []AlgorithmFunc{}}
	for _, option := range options {
		option(opts)
	}

	algo := &Algorithm{
		Name:        name,
		Version:     version,
		Description: description,
		WindowType:  windowType,
		ExecFunc:    execFunc,
		Processor:   p.name,
		Runtime:     p.runtime,
		ResultType:  resultType,
	}

	fullName := algo.FullName()
	if err := p.registry.addAlgorithm(fullName, algo); err != nil {
		return err
	}

	p.registry.addWindowTrigger(algo.FullWindowName(), algo)

	for _, dep := range opts.DependsOn {
		isRemote := isRemoteAlgorithm(dep)
		if !isRemote && !p.registry.hasAlgorithmFunc(dep) {
			return InvalidDependencyError{"dependency must be registered before use"}
		}
		if err := p.registry.addDependency(fullName, dep, isRemote); err != nil {
			return err
		}
	}

	return nil
}

// ExecuteDagPart implements the gRPC method
func (p *Processor) ExecuteDagPart(req *contract.ExecutionRequest, stream contract.OrcaProcessor_ExecuteDagPartServer) error {
	log.Info().Msg(fmt.Sprintf("Received DAG execution request with %d algorithms and ExecId: %s",
		len(req.AlgorithmExecutions), req.ExecId))

	// ctx := stream.Context()
	// FIXME: Feed back in to the algorithm
	results := make(chan *contract.ExecutionResult, len(req.AlgorithmExecutions))
	var wg sync.WaitGroup

	for _, algoExec := range req.AlgorithmExecutions {
		wg.Add(1)
		go func(exec *contract.ExecuteAlgorithm) {
			defer wg.Done()
			result := p.executeAlgorithm(req.ExecId, exec.Algorithm, req.Window, exec.Dependencies)
			results <- result
		}(algoExec)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		if err := stream.Send(result); err != nil {
			log.Error().Err(err).Msg("Failed to send result")
			return err
		}
	}

	return nil
}

func (p *Processor) executeAlgorithm(execID string, algorithm *contract.Algorithm, window *contract.Window, dependencies []*contract.AlgorithmDependencyResult) *contract.ExecutionResult {
	log.Debug().Str("name", algorithm.Name).Str("version", algorithm.Version).Msg("Processing algorithm: %s_%s")

	algoName := fmt.Sprintf("%s_%s", algorithm.Name, algorithm.Version)
	p.registry.mu.RLock()
	algo, exists := p.registry.algorithms[algoName]
	p.registry.mu.RUnlock()

	if !exists {
		return p.createErrorResult(execID, algorithm, fmt.Errorf("algorithm not found: %s", algoName))
	}

	// Build dependencies
	deps := NewDependencies()
	for _, depResult := range dependencies {
		depAlgo := DependencyAlgorithm{
			Name:        depResult.Algorithm.Name,
			Version:     depResult.Algorithm.Version,
			Description: depResult.Algorithm.Description,
		}

		var rows []DependencyResultRow
		for _, res := range depResult.Result {
			var value any
			switch v := res.Result.ResultData.(type) {
			case *contract.Result_SingleValue:
				value = v.SingleValue
			case *contract.Result_FloatValues:
				vals := make([]any, len(v.FloatValues.Values))
				for i, f := range v.FloatValues.Values {
					vals[i] = f
				}
				value = vals
			case *contract.Result_StructValue:
				value = v.StructValue.AsMap()
			}

			rows = append(rows, DependencyResultRow{
				Result: value,
				Window: Window{
					TimeFrom: res.Window.TimeFrom.AsTime(),
					TimeTo:   res.Window.TimeTo.AsTime(),
					Name:     res.Window.WindowTypeName,
					Version:  res.Window.WindowTypeVersion,
					Origin:   res.Window.Origin,
				},
			})
		}

		depRes := &DependencyResult{
			Algorithm: depAlgo,
			Results:   rows,
		}
		deps.deps[depAlgo.ID()] = depRes
	}

	params := NewExecutionParams(window, deps)

	// Execute algorithm
	result, err := algo.ExecFunc(params)
	if err != nil {
		return p.createErrorResult(execID, algorithm, err)
	}

	return p.createSuccessResult(execID, algorithm, result)
}

func (p *Processor) createErrorResult(execID string, algorithm *contract.Algorithm, err error) *contract.ExecutionResult {
	log.Error().Err(err).Str("name", algorithm.Name).Msg("Algorithm %s failed")

	errorStruct, _ := structpb.NewStruct(map[string]any{
		"error": err.Error(),
	})
	errorStructPb := contract.Result_StructValue{
		StructValue: errorStruct,
	}

	return &contract.ExecutionResult{
		ExecId: execID,
		AlgorithmResult: &contract.AlgorithmResult{
			Algorithm: algorithm,
			Result: &contract.Result{
				Status:     contract.ResultStatus_RESULT_STATUS_UNHANDLED_FAILED,
				ResultData: &errorStructPb,
				Timestamp:  time.Now().Unix(),
			},
		},
	}
}

func (p *Processor) createSuccessResult(execID string, algorithm *contract.Algorithm, result Result) *contract.ExecutionResult {
	pbResult := &contract.Result{
		Status:    contract.ResultStatus_RESULT_STATUS_SUCEEDED,
		Timestamp: time.Now().Unix(),
	}

	switch r := result.(type) {
	case StructResult:
		structVal, _ := structpb.NewStruct(r.Value)
		pbResult.ResultData = &contract.Result_StructValue{StructValue: structVal}
	case ValueResult:
		if f, ok := r.Value.(Float64Value); ok {
			pbResult.ResultData = &contract.Result_SingleValue{SingleValue: float32(f)}
		} else if i, ok := r.Value.(IntValue); ok {
			pbResult.ResultData = &contract.Result_SingleValue{SingleValue: float32(i)}
		}
	case ArrayResult:
		if f, ok := r.Value.(Float64Array); ok {
			floats := make([]float32, len(f))
			for i, v := range f {
				floats[i] = float32(v)
			}
			pbResult.ResultData = &contract.Result_FloatValues{FloatValues: &contract.FloatArray{Values: floats}}
		} else if i, ok := r.Value.(IntArray); ok {
			floats := make([]float32, len(i))
			for ii, v := range i {
				floats[ii] = float32(v)
			}
			pbResult.ResultData = &contract.Result_FloatValues{FloatValues: &contract.FloatArray{Values: floats}}
		}
	}

	return &contract.ExecutionResult{
		ExecId: execID,
		AlgorithmResult: &contract.AlgorithmResult{
			Algorithm: algorithm,
			Result:    pbResult,
		},
	}
}

// HealthCheck implements the gRPC method
func (p *Processor) HealthCheck(ctx context.Context, req *contract.HealthCheckRequest) (*contract.HealthCheckResponse, error) {
	log.Debug().Msg("Received health check request")
	return &contract.HealthCheckResponse{
		Status:  contract.HealthCheckResponse_STATUS_SERVING,
		Message: "Processor is healthy",
		Metrics: &contract.ProcessorMetrics{
			ActiveTasks:   0,
			MemoryBytes:   0,
			CpuPercent:    0.0,
			UptimeSeconds: 0,
		},
	}, nil
}

// Register registers the processor with Orca Core
func (p *Processor) Register(ctx context.Context) error {
	log.Info().Str("name", p.name).Msg("Preparing to register processor '%s' with Orca Core")

	req := &contract.ProcessorRegistration{
		Name:          p.name,
		Runtime:       p.runtime,
		ConnectionStr: p.orcaProcessorConnStr,
	}

	if p.projectName != "" {
		req.ProjectName = p.projectName
	}

	p.registry.mu.RLock()
	for _, algo := range p.registry.algorithms {
		algoMsg := &contract.Algorithm{
			Name:        algo.Name,
			Version:     algo.Version,
			Description: algo.Description,
			ResultType:  algo.ResultType,
			WindowType: &contract.WindowType{
				Name:        algo.WindowType.Name,
				Version:     algo.WindowType.Version,
				Description: algo.WindowType.Description,
			},
		}

		for _, field := range algo.WindowType.MetadataFields {
			algoMsg.WindowType.MetadataFields = append(algoMsg.WindowType.MetadataFields, &contract.MetadataField{
				Name:        field.Name,
				Description: field.Description,
			})
		}

		// Add dependencies
		if deps, exists := p.registry.dependencies[algo.FullName()]; exists {
			for _, dep := range deps {
				lookback := p.registry.getLookback(algo.FullName(), dep.FullName())
				depMsg := &contract.AlgorithmDependency{
					Name:             dep.Name,
					Version:          dep.Version,
					ProcessorName:    dep.Processor,
					ProcessorRuntime: dep.Runtime,
				}
				if lookback.N > 0 {
					depMsg.Lookback = &contract.AlgorithmDependency_LookbackNum{LookbackNum: uint32(lookback.N)}
				} else if lookback.TD > 0 {
					depMsg.Lookback = &contract.AlgorithmDependency_LookbackTimeDelta{LookbackTimeDelta: uint64(lookback.TD.Nanoseconds())}
				}
				algoMsg.Dependencies = append(algoMsg.Dependencies, depMsg)
			}
		}

		// Add remote dependencies
		if remoteDeps, exists := p.registry.remoteDependencies[algo.FullName()]; exists {
			for _, remoteDep := range remoteDeps {
				lookback := p.registry.getLookback(algo.FullName(), remoteDep.FullName())
				depMsg := &contract.AlgorithmDependency{
					Name:             remoteDep.Name,
					Version:          remoteDep.Version,
					ProcessorName:    remoteDep.ProcessorName,
					ProcessorRuntime: remoteDep.ProcessorRuntime,
				}
				if lookback.N > 0 {
					depMsg.Lookback = &contract.AlgorithmDependency_LookbackNum{LookbackNum: uint32(lookback.N)}
				} else if lookback.TD > 0 {
					depMsg.Lookback = &contract.AlgorithmDependency_LookbackTimeDelta{LookbackTimeDelta: uint64(lookback.TD.Nanoseconds())}
				}
				algoMsg.Dependencies = append(algoMsg.Dependencies, depMsg)
			}
		}

		req.SupportedAlgorithms = append(req.SupportedAlgorithms, algoMsg)
	}
	p.registry.mu.RUnlock()

	var conn *grpc.ClientConn
	var err error

	if p.isProduction {
		creds := credentials.NewTLS(&tls.Config{})
		conn, err = grpc.NewClient(p.orcaCore, grpc.WithTransportCredentials(creds))
	} else {
		conn, err = grpc.NewClient(p.orcaCore, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if err != nil {
		return fmt.Errorf("failed to connect to Orca Core: %w", err)
	}
	defer conn.Close()

	client := contract.NewOrcaCoreClient(conn)
	resp, err := client.RegisterProcessor(ctx, req)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	log.Info().Str("response", resp.GetMessage()).Msg("algorithm registration response")
	return nil
}

// Start starts the gRPC server
func (p *Processor) Start() error {
	log.Info().Str("name", p.name).Str("runtime", p.runtime).Msg("starting Orca Processor")
	log.Info().Int("maxWorkers", p.maxWorkers).Msg("initialising gRPC server")
	lis, err := net.Listen("tcp", p.processorConnStr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(50*1024*1024),
		grpc.MaxSendMsgSize(50*1024*1024),
	)

	contract.RegisterOrcaProcessorServer(grpcServer, p)
	reflection.Register(grpcServer)

	log.Info().Str("processorConnStr", p.processorConnStr).Msg("server listening")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		log.Info().Msg("recieved shutdown signal, stopping server...")
		grpcServer.GracefulStop()
	}()

	log.Info().Msg("server is ready for requests")
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("server failed: %w", err)
	}

	log.Info().Msg("Server shutdown complete")
	return nil
}

// EmitWindow emits a window to Orca-core
func EmitWindow(ctx context.Context, window Window, orcaCore string, isProduction bool) error {
	log.Info().Object("window", window).Msg("Emitting window")

	pbWindow := &contract.Window{
		TimeFrom:          timestamppb.New(window.TimeFrom),
		TimeTo:            timestamppb.New(window.TimeTo),
		WindowTypeName:    window.Name,
		WindowTypeVersion: window.Version,
		Origin:            window.Origin,
	}

	if len(window.Metadata) > 0 {
		metadata, err := structpb.NewStruct(window.Metadata)
		if err != nil {
			return fmt.Errorf("failed to convert metadata: %w", err)
		}
		pbWindow.Metadata = metadata
	}

	var conn *grpc.ClientConn
	var err error

	if isProduction {
		creds := credentials.NewTLS(&tls.Config{})
		conn, err = grpc.NewClient(orcaCore, grpc.WithTransportCredentials(creds))
	} else {
		conn, err = grpc.NewClient(orcaCore, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if err != nil {
		return fmt.Errorf("failed to connect to Orca Core: %w", err)
	}
	defer conn.Close()

	client := contract.NewOrcaCoreClient(conn)
	resp, err := client.EmitWindow(ctx, pbWindow)
	if err != nil {
		return fmt.Errorf("failed to emit window: %w", err)
	}

	log.Info().Str("response", resp.GetStatus().String()).Msg("window emitted")
	return nil
}

// Helper functions

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func extractAlgorithmMeta(fn AlgorithmFunc) (name, version string) {
	// In Go, we'd need to store metadata differently
	// This is a placeholder - actual implementation would use reflection or metadata storage
	return "", ""
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

func inferResultType(fn AlgorithmFunc) contract.ResultType {
	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		return contract.ResultType_NOT_SPECIFIED
	}

	if fnType.NumOut() < 1 {
		return contract.ResultType_NOT_SPECIFIED
	}

	returnType := fnType.Out(0)
	typeName := returnType.String()

	switch {
	case strings.Contains(typeName, "StructResult"):
		return contract.ResultType_STRUCT
	case strings.Contains(typeName, "ValueResult"):
		return contract.ResultType_VALUE
	case strings.Contains(typeName, "ArrayResult"):
		return contract.ResultType_ARRAY
	case strings.Contains(typeName, "NoneResult"):
		return contract.ResultType_NONE
	default:
		return contract.ResultType_NOT_SPECIFIED
	}
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

// ApplyLookback applies lookback options (this would be used differently in Go)
func ApplyLookback(opts ...LookbackOption) lookbackParams {
	params := lookbackParams{}
	for _, opt := range opts {
		opt(&params)
	}
	return params
}
