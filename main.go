// Package orca provides a Go SDK for integrating with the Orca gRPC service
// to register, execute, and manage algorithms. Algorithms can have dependencies
// which are managed by Orca-core.
package orca

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/orc-analytics/core/protobufs/go"
)

// Validation patterns
var (
	algorithmNamePattern = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)
	semverPattern        = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`)
	windowNamePattern    = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)
)

// MetadataField represents a field in window metadata
type MetadataField struct {
	Name        string
	Description string
}

// Validate checks if the metadata field is valid
func (m *MetadataField) Validate() error {
	if m.Name == "" {
		return fmt.Errorf("metadata field name cannot be empty")
	}
	if m.Description == "" {
		return fmt.Errorf("metadata field description cannot be empty")
	}
	return nil
}

// WindowType defines the type of window that triggers algorithms
type WindowType struct {
	Name           string
	Version        string
	Description    string
	MetadataFields []MetadataField
}

// Validate checks if the window type is valid
func (w *WindowType) Validate() error {
	if !windowNamePattern.MatchString(w.Name) {
		return fmt.Errorf("window name '%s' must be in PascalCase", w.Name)
	}
	if !semverPattern.MatchString(w.Version) {
		return fmt.Errorf("window version '%s' must follow semantic versioning (e.g., '1.0.0')", w.Version)
	}

	seen := make(map[string]bool)
	for _, field := range w.MetadataFields {
		if err := field.Validate(); err != nil {
			return err
		}
		key := field.Name + field.Description
		if seen[key] {
			return fmt.Errorf("duplicate metadata field: name='%s', description='%s'", field.Name, field.Description)
		}
		seen[key] = true
	}
	return nil
}

// FullName returns the full window name as "name_version"
func (w *WindowType) FullName() string {
	return fmt.Sprintf("%s_%s", w.Name, w.Version)
}

// Window represents a time window with metadata
type Window struct {
	TimeFrom time.Time
	TimeTo   time.Time
	Name     string
	Version  string
	Origin   string
	Metadata map[string]interface{}
}

// ExecutionParams contains the parameters passed to algorithm execution
type ExecutionParams struct {
	Window       Window
	Dependencies map[string]interface{}
}

// Result types for algorithm returns
type (
	// StructResult represents a structured dictionary result
	StructResult struct {
		Value map[string]interface{}
	}

	// ValueResult represents a single numeric or boolean value
	ValueResult struct {
		Value interface{} // float64, int, or bool
	}

	// ArrayResult represents an array of numeric or boolean values
	ArrayResult struct {
		Value []interface{} // []float64, []int, or []bool
	}

	// NoneResult represents no result
	NoneResult struct{}
)

// AlgorithmFunc is the signature for algorithm execution functions
type AlgorithmFunc func(params ExecutionParams) (interface{}, error)

// Algorithm represents a registered algorithm
type Algorithm struct {
	Name        string
	Version     string
	Description string
	WindowType  WindowType
	ExecFn      AlgorithmFunc
	Processor   string
	Runtime     string
	ResultType  pb.ResultType
	DependsOn   []string // Full names of dependencies
}

// FullName returns the algorithm's full name as "name_version"
func (a *Algorithm) FullName() string {
	return fmt.Sprintf("%s_%s", a.Name, a.Version)
}

// algorithmRegistry manages all registered algorithms
type algorithmRegistry struct {
	mu                 sync.RWMutex
	algorithms         map[string]*Algorithm
	windowTriggers     map[string][]*Algorithm
	dependencies       map[string][]string
	remoteDependencies map[string][]RemoteAlgorithm
}

// RemoteAlgorithm represents a dependency on a remote algorithm
type RemoteAlgorithm struct {
	ProcessorName    string
	ProcessorRuntime string
	Name             string
	Version          string
}

func newAlgorithmRegistry() *algorithmRegistry {
	return &algorithmRegistry{
		algorithms:         make(map[string]*Algorithm),
		windowTriggers:     make(map[string][]*Algorithm),
		dependencies:       make(map[string][]string),
		remoteDependencies: make(map[string][]RemoteAlgorithm),
	}
}

func (r *algorithmRegistry) add(algo *Algorithm) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.algorithms[algo.FullName()]; exists {
		return fmt.Errorf("algorithm %s already registered", algo.FullName())
	}

	r.algorithms[algo.FullName()] = algo

	windowKey := algo.WindowType.FullName()
	r.windowTriggers[windowKey] = append(r.windowTriggers[windowKey], algo)

	log.Printf("Registered algorithm: %s (window: %s)", algo.FullName(), windowKey)
	return nil
}

func (r *algorithmRegistry) get(fullName string) (*Algorithm, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	algo, exists := r.algorithms[fullName]
	return algo, exists
}

func (r *algorithmRegistry) all() []*Algorithm {
	r.mu.RLock()
	defer r.mu.RUnlock()

	algos := make([]*Algorithm, 0, len(r.algorithms))
	for _, algo := range r.algorithms {
		algos = append(algos, algo)
	}
	return algos
}

// Processor is the main entry point for the Orca SDK
type Processor struct {
	name                  string
	runtime               string
	maxWorkers            int
	processorPort         string
	processorExternalPort string
	processorHost         string
	orcaCore              string
	projectName           string
	isProduction          bool
	registry              *algorithmRegistry

	pb.UnimplementedOrcaProcessorServer
}

// ProcessorOption is a functional option for configuring a Processor
type ProcessorOption func(*Processor)

// WithMaxWorkers sets the maximum number of concurrent workers
func WithMaxWorkers(n int) ProcessorOption {
	return func(p *Processor) {
		p.maxWorkers = n
	}
}

// WithProjectName sets the project name for this processor
func WithProjectName(name string) ProcessorOption {
	return func(p *Processor) {
		p.projectName = name
	}
}

// NewProcessor creates a new Orca processor
func NewProcessor(name string, opts ...ProcessorOption) *Processor {
	p := &Processor{
		name:                  name,
		runtime:               fmt.Sprintf("go %s", os.Getenv("GO_VERSION")),
		maxWorkers:            10,
		processorPort:         getEnv("PROCESSOR_PORT", "50051"),
		processorExternalPort: getEnv("PROCESSOR_EXTERNAL_PORT", "50051"),
		processorHost:         getEnv("PROCESSOR_HOST", "localhost"),
		orcaCore:              getEnv("ORCA_CORE", "localhost:50050"),
		projectName:           getEnv("PROJECT_NAME", ""),
		isProduction:          getEnv("ENVIRONMENT", "development") == "production",
		registry:              newAlgorithmRegistry(),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// AlgorithmConfig contains configuration for registering an algorithm
type AlgorithmConfig struct {
	Name        string
	Version     string
	WindowType  WindowType
	Description string
	DependsOn   []string // Full names of dependencies
}

// Algorithm registers a new algorithm with the processor
func (p *Processor) Algorithm(config AlgorithmConfig, fn AlgorithmFunc) error {
	// Validate algorithm name
	if !algorithmNamePattern.MatchString(config.Name) {
		return fmt.Errorf("algorithm name '%s' must be in PascalCase", config.Name)
	}

	// Validate version
	if !semverPattern.MatchString(config.Version) {
		return fmt.Errorf("version '%s' must follow semantic versioning (e.g., '1.0.0')", config.Version)
	}

	// Validate window type
	if err := config.WindowType.Validate(); err != nil {
		return fmt.Errorf("invalid window type: %w", err)
	}

	// Determine result type (this would need runtime type inspection in production)
	// For now, we'll default to STRUCT
	resultType := pb.ResultType_STRUCT

	algo := &Algorithm{
		Name:        config.Name,
		Version:     config.Version,
		Description: config.Description,
		WindowType:  config.WindowType,
		ExecFn:      fn,
		Processor:   p.name,
		Runtime:     p.runtime,
		ResultType:  resultType,
		DependsOn:   config.DependsOn,
	}

	return p.registry.add(algo)
}

// Register registers this processor and all its algorithms with Orca Core
func (p *Processor) Register(ctx context.Context) error {
	log.Printf("Preparing to register processor '%s' with Orca Core", p.name)

	req := &pb.ProcessorRegistration{
		Name:          p.name,
		Runtime:       p.runtime,
		ConnectionStr: fmt.Sprintf("%s:%s", p.processorHost, p.processorExternalPort),
		ProjectName:   p.projectName,
	}

	// Add all algorithms
	for _, algo := range p.registry.all() {
		algoPb := &pb.Algorithm{
			Name:        algo.Name,
			Version:     algo.Version,
			Description: algo.Description,
			ResultType:  algo.ResultType,
			WindowType: &pb.WindowType{
				Name:        algo.WindowType.Name,
				Version:     algo.WindowType.Version,
				Description: algo.WindowType.Description,
			},
		}

		// Add metadata fields
		for _, field := range algo.WindowType.MetadataFields {
			algoPb.WindowType.MetadataFields = append(algoPb.WindowType.MetadataFields, &pb.MetadataField{
				Name:        field.Name,
				Description: field.Description,
			})
		}

		// Add dependencies
		for _, depName := range algo.DependsOn {
			if dep, ok := p.registry.get(depName); ok {
				algoPb.Dependencies = append(algoPb.Dependencies, &pb.AlgorithmDependency{
					Name:             dep.Name,
					Version:          dep.Version,
					ProcessorName:    dep.Processor,
					ProcessorRuntime: dep.Runtime,
				})
			}
		}

		req.SupportedAlgorithms = append(req.SupportedAlgorithms, algoPb)
	}

	// Connect to Orca Core
	conn, err := p.connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Orca Core: %w", err)
	}
	defer conn.Close()

	client := pb.NewOrcaCoreClient(conn)
	resp, err := client.RegisterProcessor(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to register processor: %w", err)
	}

	log.Printf("Algorithm registration response received: %v", resp)
	return nil
}

// Start starts the gRPC server and begins serving algorithm requests
func (p *Processor) Start(ctx context.Context) error {
	log.Printf("Starting Orca Processor '%s' with %s", p.name, p.runtime)
	log.Printf("Initializing gRPC server with %d workers", p.maxWorkers)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", p.processorPort))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(50*1024*1024), // 50MB
		grpc.MaxSendMsgSize(50*1024*1024), // 50MB
	)

	pb.RegisterOrcaProcessorServer(grpcServer, p)
	reflection.Register(grpcServer)

	log.Printf("Server listening on :%s", p.processorPort)

	// Setup graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Received shutdown signal, stopping server...")
		grpcServer.GracefulStop()
	}()

	log.Println("Server is ready for requests")
	return grpcServer.Serve(lis)
}

// ExecuteDagPart implements the gRPC method for executing part of a DAG
func (p *Processor) ExecuteDagPart(req *pb.ExecutionRequest, stream pb.OrcaProcessor_ExecuteDagPartServer) error {
	log.Printf("Received DAG execution request with %d algorithms (ExecId: %s)",
		len(req.Algorithms), req.ExecId)

	ctx := stream.Context()

	// Create a wait group and result channel
	var wg sync.WaitGroup
	resultCh := make(chan *pb.ExecutionResult, len(req.Algorithms))

	// Execute all algorithms concurrently
	for _, algo := range req.Algorithms {
		wg.Add(1)
		go func(algorithm *pb.Algorithm) {
			defer wg.Done()
			result := p.executeAlgorithm(ctx, req.ExecId, algorithm, req)
			resultCh <- result
		}(algo)
	}

	// Close result channel when all goroutines complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Stream results as they complete
	for result := range resultCh {
		if err := stream.Send(result); err != nil {
			return fmt.Errorf("failed to send result: %w", err)
		}
	}

	return nil
}

// executeAlgorithm executes a single algorithm
func (p *Processor) executeAlgorithm(ctx context.Context, execID string, algoPb *pb.Algorithm, req *pb.ExecutionRequest) *pb.ExecutionResult {
	algoName := fmt.Sprintf("%s_%s", algoPb.Name, algoPb.Version)

	algo, ok := p.registry.get(algoName)
	if !ok {
		return p.createErrorResult(execID, algoPb, fmt.Errorf("algorithm not found: %s", algoName))
	}

	// Build execution params
	params := ExecutionParams{
		Window: Window{
			TimeFrom: req.Window.TimeFrom.AsTime(),
			TimeTo:   req.Window.TimeTo.AsTime(),
			Name:     req.Window.WindowTypeName,
			Version:  req.Window.WindowTypeVersion,
			Origin:   req.Window.Origin,
			Metadata: make(map[string]interface{}),
		},
		Dependencies: make(map[string]interface{}),
	}

	// Parse window metadata
	if req.Window.Metadata != nil {
		params.Window.Metadata = req.Window.Metadata.AsMap()
	}

	// Parse dependencies
	for _, depResult := range req.AlgorithmResults {
		depName := fmt.Sprintf("%s_%s", depResult.Algorithm.Name, depResult.Algorithm.Version)

		switch v := depResult.Result.Result.(type) {
		case *pb.Result_SingleValue:
			params.Dependencies[depName] = v.SingleValue
		case *pb.Result_FloatValues:
			params.Dependencies[depName] = v.FloatValues.Values
		case *pb.Result_StructValue:
			params.Dependencies[depName] = v.StructValue.AsMap()
		}
	}

	// Execute algorithm
	result, err := algo.ExecFn(params)
	if err != nil {
		return p.createErrorResult(execID, algoPb, err)
	}

	// Convert result to protobuf
	resultPb, err := p.convertResult(result, algo.ResultType)
	if err != nil {
		return p.createErrorResult(execID, algoPb, err)
	}

	log.Printf("Completed algorithm: %s", algoPb.Name)

	return &pb.ExecutionResult{
		ExecId: execID,
		AlgorithmResult: &pb.AlgorithmResult{
			Algorithm: algoPb,
			Result:    resultPb,
		},
	}
}

// convertResult converts a Go result to a protobuf Result
func (p *Processor) convertResult(result interface{}, resultType pb.ResultType) (*pb.Result, error) {
	switch v := result.(type) {
	case StructResult:
		structPb, err := structpb.NewStruct(v.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert struct result: %w", err)
		}
		return &pb.Result{
			Status: pb.ResultStatus_RESULT_STATUS_SUCEEDED,
			Result: &pb.Result_StructValue{StructValue: structPb},
		}, nil

	case ValueResult:
		var floatVal float64
		switch val := v.Value.(type) {
		case float64:
			floatVal = val
		case int:
			floatVal = float64(val)
		case bool:
			if val {
				floatVal = 1.0
			} else {
				floatVal = 0.0
			}
		default:
			return nil, fmt.Errorf("unsupported value type: %T", val)
		}
		return &pb.Result{
			Status: pb.ResultStatus_RESULT_STATUS_SUCEEDED,
			Result: &pb.Result_SingleValue{SingleValue: floatVal},
		}, nil

	case ArrayResult:
		floats := make([]float64, len(v.Value))
		for i, val := range v.Value {
			switch val := val.(type) {
			case float64:
				floats[i] = val
			case int:
				floats[i] = float64(val)
			case bool:
				if val {
					floats[i] = 1.0
				} else {
					floats[i] = 0.0
				}
			default:
				return nil, fmt.Errorf("unsupported array element type: %T", val)
			}
		}
		return &pb.Result{
			Status: pb.ResultStatus_RESULT_STATUS_SUCEEDED,
			Result: &pb.Result_FloatValues{FloatValues: &pb.FloatArray{Values: floats}},
		}, nil

	case NoneResult:
		return &pb.Result{
			Status: pb.ResultStatus_RESULT_STATUS_SUCEEDED,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported result type: %T", result)
	}
}

// createErrorResult creates an error execution result
func (p *Processor) createErrorResult(execID string, algo *pb.Algorithm, err error) *pb.ExecutionResult {
	log.Printf("Algorithm %s failed: %v", algo.Name, err)

	errorStruct, _ := structpb.NewStruct(map[string]interface{}{
		"error": err.Error(),
	})

	return &pb.ExecutionResult{
		ExecId: execID,
		AlgorithmResult: &pb.AlgorithmResult{
			Algorithm: algo,
			Result: &pb.Result{
				Status:    pb.ResultStatus_RESULT_STATUS_UNHANDLED_FAILED,
				Timestamp: time.Now().Unix(),
				Result:    &pb.Result_StructValue{StructValue: errorStruct},
			},
		},
	}
}

// HealthCheck implements the health check gRPC method
func (p *Processor) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Status:  pb.HealthCheckResponse_STATUS_SERVING,
		Message: "Processor is healthy",
		Metrics: &pb.ProcessorMetrics{
			ActiveTasks:   0,
			MemoryBytes:   0,
			CpuPercent:    0.0,
			UptimeSeconds: 0,
		},
	}, nil
}

// connect creates a connection to Orca Core
func (p *Processor) connect(ctx context.Context) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if p.isProduction {
		creds := credentials.NewTLS(nil)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	return grpc.DialContext(ctx, p.orcaCore, opts...)
}

// EmitWindow emits a window to Orca Core
func EmitWindow(ctx context.Context, window Window) error {
	log.Printf("Emitting window: %+v", window)

	// Get environment configuration
	orcaCore := getEnv("ORCA_CORE", "localhost:50050")
	isProduction := getEnv("ENVIRONMENT", "development") == "production"

	// Connect to Orca Core
	var opts []grpc.DialOption
	if isProduction {
		creds := credentials.NewTLS(nil)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, orcaCore, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to Orca Core: %w", err)
	}
	defer conn.Close()

	// Create window protobuf
	metadata, err := structpb.NewStruct(window.Metadata)
	if err != nil {
		return fmt.Errorf("failed to convert metadata: %w", err)
	}

	windowPb := &pb.Window{
		TimeFrom:          timestamppb.New(window.TimeFrom),
		TimeTo:            timestamppb.New(window.TimeTo),
		WindowTypeName:    window.Name,
		WindowTypeVersion: window.Version,
		Origin:            window.Origin,
		Metadata:          metadata,
	}

	// Emit window
	client := pb.NewOrcaCoreClient(conn)
	resp, err := client.EmitWindow(ctx, windowPb)
	if err != nil {
		return fmt.Errorf("failed to emit window: %w", err)
	}

	log.Printf("Window emitted: %v", resp)
	return nil
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
