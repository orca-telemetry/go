package orca_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sdk "github.com/orca-telemetry/sdk"
	"github.com/stretchr/testify/assert"
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
