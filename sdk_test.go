package orca_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	sdk "github.com/orca-telemetry/go"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
)

// define some algorithms
func returnNone(params *sdk.ExecutionParams) (sdk.Result, error) {
	return sdk.NoneResult{}, nil
}

func returnStruct(params *sdk.ExecutionParams) (sdk.Result, error) {
	sampleStruct := make(map[string]any, 10)
	for ii := range 10 {
		sampleStruct[fmt.Sprintf("metric_%d", ii)] = float64(ii)
	}
	return sdk.StructResult{
		Value: sampleStruct,
	}, nil
}

func returnValue(params *sdk.ExecutionParams) (sdk.Result, error) {
	return sdk.ValueResult{
		Value: 1.0}, nil
}

func TestAlgorithmsRegistered(t *testing.T) {
	proc := sdk.NewProcessor("test_processor", sdk.WithOrcaCore(orcaConnStr))
	windowType, err := sdk.NewWindowType(
		"MyTriggeringWindow",
		"1.0.0",
		"A simple triggering window for testing purposes",
		[]sdk.MetadataField{
			{Name: "asset_id", Description: "id of the asset"},
			{Name: "cycles", Description: "an arbitrary field"},
		},
	)
	assert.NoError(t, err)

	noneAlgo, err := sdk.NewAlgorithm(
		"NoneAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		windowType,
		returnNone,
		sdk.KindNone,
	)
	assert.NoError(t, err)

	structAlgo, err := sdk.NewAlgorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		windowType,
		returnStruct,
		sdk.KindStruct,
		sdk.WithDependencies([]sdk.Dependency{{
			Algorithm: noneAlgo,
		}}),
	)
	assert.NoError(t, err)
	err = proc.Register(context.Background(), []*sdk.Algorithm{noneAlgo, structAlgo})
	assert.NoError(t, err)
}

func TestAlgorithmAlreadyExists(t *testing.T) {
	proc := sdk.NewProcessor("test_processor", sdk.WithOrcaCore(orcaConnStr))

	var err error
	window, err := sdk.NewWindowType(
		"MyTriggeringWindow",
		"1.0.0",
		"A simple triggering window for testing purposes",
		[]sdk.MetadataField{
			{Name: "asset_id", Description: "id of the asset"},
			{Name: "cycles", Description: "an arbitrary field"},
		},
	)
	assert.NoError(t, err)

	structAlgo, err := sdk.NewAlgorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		window,
		returnStruct,
		sdk.KindStruct,
	)
	assert.NoError(t, err)

	noneAlgo, err := sdk.NewAlgorithm(
		"NoneAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		window,
		returnNone,
		sdk.KindNone,
		sdk.WithDependencies([]sdk.Dependency{{
			Algorithm: structAlgo,
		},
		}),
	)
	assert.NoError(t, err)

	err = proc.Register(context.Background(), []*sdk.Algorithm{
		noneAlgo, structAlgo, structAlgo,
	})
	var targetErr sdk.InvalidAlgorithmArgumentError
	assert.ErrorAs(t, err, &targetErr)
}

func TestDependencyWithLookbackWorks(t *testing.T) {

	proc := sdk.NewProcessor("test_processor", sdk.WithOrcaCore(orcaConnStr))
	windowType, err := sdk.NewWindowType(
		"MyTriggeringWindow",
		"1.0.0",
		"A simple triggering window for testing purposes",
		[]sdk.MetadataField{
			{Name: "asset_id", Description: "id of the asset"},
			{Name: "cycles", Description: "an arbitrary field"},
		},
	)
	assert.NoError(t, err)

	noneAlgo, err := sdk.NewAlgorithm(
		"NoneAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		windowType,
		returnNone,
		sdk.KindNone,
	)

	assert.NoError(t, err)

	structAlgo, err := sdk.NewAlgorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		windowType,
		returnStruct,
		sdk.KindStruct,
		sdk.WithDependencies(
			[]sdk.Dependency{
				{
					Algorithm: noneAlgo,
					Lookback:  sdk.WithLookbackTD(time.Hour * 24 * time.Duration(20)),
				},
			},
		),
	)

	assert.NoError(t, err)

	err = proc.Register(context.Background(), []*sdk.Algorithm{
		noneAlgo, structAlgo,
	})
	assert.NoError(t, err)
}

func TestComplexDagFires(t *testing.T) {
	proc := sdk.NewProcessor("test_processor", sdk.WithOrcaCore(orcaConnStr), sdk.WithProcessorHost(testcontainers.HostInternal))
	// TODO: Assert that the dependency results are being fed through to the algorithms

	windowType, err := sdk.NewWindowType(
		"MyTriggeringWindow",
		"1.0.0",
		"A simple triggering window for testing purposes",
		[]sdk.MetadataField{
			{Name: "asset_id", Description: "id of the asset"},
			{Name: "cycles", Description: "an arbitrary field"},
		},
	)
	assert.NoError(t, err)

	// define some algorithms
	var wg sync.WaitGroup
	wg.Add(3) // one for each function

	returnNone := func(params *sdk.ExecutionParams) (sdk.Result, error) {
		defer wg.Done()
		return sdk.NoneResult{}, nil
	}

	noneAlgo, err := sdk.NewAlgorithm(
		"NoneAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		windowType,
		returnNone,
		sdk.KindNone,
	)

	assert.NoError(t, err)
	returnValue := func(params *sdk.ExecutionParams) (sdk.Result, error) {
		defer wg.Done()
		return sdk.ValueResult{
			Value: 1.0,
		}, nil
	}
	valueAlgo, err := sdk.NewAlgorithm(
		"ValueAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy value",
		windowType,
		returnValue,
		sdk.KindStruct,
		sdk.WithDependencies(
			[]sdk.Dependency{
				{
					Algorithm: noneAlgo,
					Lookback:  sdk.WithLookbackTD(time.Hour * 24 * time.Duration(20)),
				},
			},
		),
	)
	assert.NoError(t, err)

	returnStruct := func(params *sdk.ExecutionParams) (sdk.Result, error) {
		sampleStruct := make(map[string]any, 10)
		for ii := range 10 {
			sampleStruct[fmt.Sprintf("metric_%d", ii)] = float64(ii)
		}
		defer wg.Done()
		return sdk.StructResult{
			Value: sampleStruct,
		}, nil
	}
	structAlgo, err := sdk.NewAlgorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		windowType,
		returnStruct,
		sdk.KindStruct,
		sdk.WithDependencies(
			[]sdk.Dependency{
				{
					Algorithm: noneAlgo,
					Lookback:  sdk.WithLookbackTD(time.Hour * 24 * time.Duration(20)),
				},
				{
					Algorithm: valueAlgo,
					Lookback:  sdk.WithLookbackN(10),
				},
			},
		),
	)

	assert.NoError(t, err)

	err = proc.Register(context.Background(), []*sdk.Algorithm{
		noneAlgo, structAlgo, valueAlgo,
	})

	assert.NoError(t, err)

	go func(proc *sdk.Processor) {
		err := proc.Start(t.Context())
		if err != nil {
			assert.NoError(t, err)
		}
	}(proc)
	tFrom := time.Now()
	tTo := tFrom.Add(time.Duration(1) * time.Minute)

	md := make(map[string]any, 2)

	md["asset_id"] = 1
	md["cycles"] = 2

	window, err := sdk.NewWindow(*windowType, tFrom, tTo, "test", md)
	assert.NoError(t, err)
	err = sdk.EmitWindow(t.Context(), window, orcaConnStr, false)
	assert.NoError(t, err)

	// wait for algos to have fired
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for algos")
	}
}
