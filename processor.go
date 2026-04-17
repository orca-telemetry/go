package orca

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	contract "github.com/orca-telemetry/contract/go"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/structpb"
)

// Processor is the entry point for registering algorithms in Orca
type Processor struct {
	contract.UnimplementedOrcaProcessorServer

	name                       string
	processorConnStr           string
	orcaProcessorConnStr       string
	runtime                    string
	maxWorkers                 int
	registry                   *algorithmRegistry
	orcaCore                   string
	isProduction               bool
	projectName                string
	processorPort              string
	processorHost              string
	processorExternalPort      string
	erroredDuringConfiguration bool
}

// ProcessorOption configures a Processor with optional
// arguments
type ProcessorOption func(*Processor)

// WithMaxWorkers sets the maximum number of concurrent workers
// to be used when running algorithms
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

// WithProduction enables TLS on the gRPC connection to
// Orca core
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
		name:                       name,
		runtime:                    runtime.Version(),
		maxWorkers:                 10,
		registry:                   newAlgorithmRegistry(),
		orcaCore:                   getEnv("ORCA_CORE", "localhost:50051"),
		isProduction:               getEnv("ORCA_ENV", "development") == "production",
		projectName:                getEnv("PROJECT_NAME", ""),
		processorPort:              getEnv("PROCESSOR_PORT", "50052"),
		processorHost:              getEnv("PROCESSOR_HOST", "localhost"),
		processorExternalPort:      getEnv("PROCESSOR_EXTERNAL_PORT", "50052"),
		erroredDuringConfiguration: false,
	}

	for _, opt := range opts {
		opt(p)
	}

	p.processorConnStr = fmt.Sprintf(":%s", p.processorPort)
	p.orcaProcessorConnStr = fmt.Sprintf("%s:%s", p.processorHost, p.processorExternalPort)

	return p
}

// ExecuteDagPart implements the gRPC ExecuteDagPart method
func (p *Processor) ExecuteDagPart(
	req *contract.ExecutionRequest,
	stream contract.OrcaProcessor_ExecuteDagPartServer) error {
	log.Info().Int("numAlgorithms", len(req.AlgorithmExecutions)).Str("executionId", req.ExecId).Msg("Received DAG execution request")

	type Outcome struct {
		Result *contract.ExecutionResult
		Error  error
	}
	outcomes := make(chan Outcome, len(req.AlgorithmExecutions))
	var wg sync.WaitGroup

	for _, algoExec := range req.AlgorithmExecutions {
		wg.Add(1)
		go func(exec *contract.ExecuteAlgorithm) {
			defer wg.Done()
			// if an error is produced here, this is not a result of the algorithm failing
			// a failing algorithm produces an "Error Result"
			// this is a catastrophic issue that should never happen
			result, err := p.executeAlgorithm(req.ExecId, exec.Algorithm, req.Window, exec.Dependencies, exec.SelfResults)
			outcomes <- Outcome{
				Result: result,
				Error:  err,
			}
		}(algoExec)
	}

	go func() {
		wg.Wait()
		close(outcomes)
	}()

	for o := range outcomes {
		if o.Error != nil {
			// if there was an unhandled error, this is catastrophic - stop exectuion
			log.Error().Err(o.Error).Msg("unhandled error when executing algorithm")
			return o.Error
		}
		if err := stream.Send(o.Result); err != nil {
			log.Error().Err(err).Msg("failed to send result to orca core")
			return err
		}
	}

	return nil
}

func (p *Processor) executeAlgorithm(
	execID string,
	algorithm *contract.Algorithm,
	window *contract.Window,
	dependencies []*contract.AlgorithmDependencyResult,
	selfResults []*contract.AlgorithmDependencyResultRow,
) (*contract.ExecutionResult, error) {
	log.Debug().Str("name", algorithm.Name).Str("version", algorithm.Version).Msg("Processing algorithm: %s_%s")

	algoName := fmt.Sprintf("%s_%s", algorithm.Name, algorithm.Version)
	p.registry.mu.RLock()
	algo, exists := p.registry.algorithms[algoName]
	p.registry.mu.RUnlock()

	if !exists {
		return p.createErrorResult(execID, algorithm, fmt.Errorf("algorithm not found: %s", algoName)), nil
	}

	// build dependencies
	deps := NewDependencies()
	for _, depResult := range dependencies {
		depAlgo := DependencyAlgorithm{
			Name:        depResult.Algorithm.Name,
			Version:     depResult.Algorithm.Version,
			Description: depResult.Algorithm.Description,
		}

		var rows []DependencyResultRow
		for _, res := range depResult.Result {
			var value Result
			switch v := res.Result.ResultData.(type) {

			case *contract.Result_SingleValue:
				value = ValueResult{
					Value: v.SingleValue,
				}

			case *contract.Result_FloatValues:
				vals := make([]float32, len(v.FloatValues.Values))

				copy(vals, v.FloatValues.GetValues())

				value = ArrayResult{
					Value: vals,
				}

			case *contract.Result_StructValue:
				value = StructResult{
					Value: v.StructValue.AsMap(),
				}

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

	// if there are self results pack then in
	var selfResultsPacked = make([]*DependencyResultRow, len(selfResults))
	for ii, res := range selfResults {
		selfResultsPacked[ii] = &DependencyResultRow{
			Window: Window{
				TimeFrom: res.Window.GetTimeFrom().AsTime(),
				TimeTo:   res.Window.GetTimeTo().AsTime(),
				Name:     res.Window.GetWindowTypeName(),
				Version:  res.Window.GetWindowTypeVersion(),
				Origin:   res.Window.GetOrigin(),
				Metadata: res.Window.GetMetadata().AsMap(),
			},
		}
	}

	params := NewExecutionParams(window, deps, selfResultsPacked)

	// execute algorithm
	result, err := algo.ExecFunc(params)
	if err != nil {
		return p.createErrorResult(execID, algorithm, err), nil
	}

	res, err := p.createSuccessResult(execID, algorithm, result)
	if err != nil {
		return nil, err
	}

	return res, nil
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

func (p *Processor) createSuccessResult(execID string, algorithm *contract.Algorithm, result Result) (*contract.ExecutionResult, error) {
	pbResult := &contract.Result{
		Status:    contract.ResultStatus_RESULT_STATUS_SUCEEDED,
		Timestamp: time.Now().Unix(),
	}

	switch r := result.(type) {
	case StructResult:
		structVal, err := structpb.NewStruct(r.Value)
		if err != nil {
			return nil, err
		}
		pbResult.ResultData = &contract.Result_StructValue{StructValue: structVal}
	case ValueResult:
		pbResult.ResultData = &contract.Result_SingleValue{SingleValue: r.Value}
	case ArrayResult:
		pbResult.ResultData = &contract.Result_FloatValues{FloatValues: &contract.FloatArray{Values: r.Value}}
	}

	return &contract.ExecutionResult{
		ExecId: execID,
		AlgorithmResult: &contract.AlgorithmResult{
			Algorithm: algorithm,
			Result:    pbResult,
		},
	}, nil
}

// HealthCheck implements the gRPC HealthCheck Method
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
func (p *Processor) Register(ctx context.Context, algorithms []*Algorithm) error {
	log.Info().Str("name", p.name).Msg("preparing to register processor with Orca Core")

	var err error

	req := &contract.ProcessorRegistration{
		Name:          p.name,
		Runtime:       p.runtime,
		ConnectionStr: p.orcaProcessorConnStr,
		ProjectName:   p.projectName,
	}

	// add each algorithm to the registry
	for _, algo := range algorithms {
		err = p.registry.addAlgorithm(algo)
		if err != nil {
			return err
		}
	}

	// lock after writing
	p.registry.mu.RLock()

	for _, algo := range p.registry.algorithms {
		var algoMsg *contract.Algorithm
		if algo.SelfLookbackTd > 0 {
			// Td takes priority
			algoMsg = &contract.Algorithm{
				Name:        algo.Name,
				Version:     algo.Version,
				Description: algo.Description,
				ResultType:  algo.ResultType,
				WindowType: &contract.WindowType{
					Name:        algo.WindowType.Name,
					Version:     algo.WindowType.Version,
					Description: algo.WindowType.Description,
				},
				Lookback: &contract.Algorithm_LookbackTimeDelta{
					LookbackTimeDelta: uint64(algo.SelfLookbackTd.Nanoseconds()),
				},
			}
		} else {
			// Td takes priority
			algoMsg = &contract.Algorithm{
				Name:        algo.Name,
				Version:     algo.Version,
				Description: algo.Description,
				ResultType:  algo.ResultType,
				WindowType: &contract.WindowType{
					Name:        algo.WindowType.Name,
					Version:     algo.WindowType.Version,
					Description: algo.WindowType.Description,
				},
				Lookback: &contract.Algorithm_LookbackNum{
					LookbackNum: uint32(algo.SelfLookbackN),
				},
			}
		}

		for _, field := range algo.WindowType.MetadataFields {
			algoMsg.WindowType.MetadataFields = append(algoMsg.WindowType.MetadataFields, &contract.MetadataField{
				Name:        field.Name,
				Description: field.Description,
			})
		}

		// add dependencies
		if deps, exists := p.registry.dependencies[algo.FullName()]; exists {
			for _, dep := range deps {
				lookback := p.registry.getLookback(algo.FullName(), dep.FullName())
				depMsg := &contract.AlgorithmDependency{
					Name:             dep.Name,
					Version:          dep.Version,
					ProcessorName:    p.name,
					ProcessorRuntime: p.runtime,
				}

				if lookback.N > 0 {
					depMsg.Lookback = &contract.AlgorithmDependency_LookbackNum{LookbackNum: uint32(lookback.N)}
				} else if lookback.TD > 0 {
					depMsg.Lookback = &contract.AlgorithmDependency_LookbackTimeDelta{LookbackTimeDelta: uint64(lookback.TD.Nanoseconds())}
				}
				algoMsg.Dependencies = append(algoMsg.Dependencies, depMsg)
			}
		}

		// add remote dependencies
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
func (p *Processor) Start(ctx context.Context) error {
	log.Info().Str("name", p.name).Str("runtime", p.runtime).Msg("starting Orca Processor")
	log.Info().Int("maxWorkers", p.maxWorkers).Msg("initialising gRPC server")

	var err error

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
	contextChan := ctx.Done()

	go func() {
		<-sigChan
		log.Info().Msg("recieved shutdown signal, stopping server...")
		grpcServer.GracefulStop()
	}()

	go func() {
		<-contextChan
		log.Info().Msg("context completed, stopping server...")
		grpcServer.GracefulStop()
	}()

	log.Info().Msg("server is ready for requests")
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("server failed: %w", err)
	}

	log.Info().Msg("Server shutdown complete")
	return nil
}
