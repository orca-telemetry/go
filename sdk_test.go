package orca_test

import (
	"context"
	"fmt"
	"testing"

	sdk "github.com/orca-telemetry/sdk"
	"github.com/stretchr/testify/assert"
)

// define some algorithms
func noneAlgo(params *sdk.ExecutionParams) (sdk.Result, error) {
	return sdk.NoneResult{}, nil
}

func structAlgo(params *sdk.ExecutionParams) (sdk.Result, error) {
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
	window := sdk.WindowType{
		Name:        "MyTriggeringWindow",
		Version:     "1.0.0",
		Description: "A simple triggering window for testing purposes",
		MetadataFields: []sdk.MetadataField{
			{Name: "asset_id", Description: "id of the asset"},
			{Name: "cycles", Description: "an arbitrary field"},
		},
	}

	err := proc.Algorithm(
		"NoneAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		window,
		noneAlgo, sdk.KindNone,
	)

	assert.NoError(t, err)

	err = proc.Algorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		window,
		structAlgo,
		sdk.KindStruct,
		sdk.WithDependencies([]sdk.Dependency{{
			Algorithm: noneAlgo,
		},
		}),
	)
	assert.NoError(t, err)

	err = proc.Register(context.Background())
	assert.NoError(t, err)
}

func TestAlgorithmAlreadyExists(t *testing.T) {
	proc := sdk.NewProcessor("test_processor", sdk.WithOrcaCore(orcaConnStr))

	window := sdk.WindowType{
		Name:        "MyTriggeringWindow",
		Version:     "1.0.0",
		Description: "A simple triggering window for testing purposes",
		MetadataFields: []sdk.MetadataField{
			{Name: "asset_id", Description: "id of the asset"},
			{Name: "cycles", Description: "an arbitrary field"},
		},
	}
	var err error
	err = proc.Algorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		window,
		structAlgo,
		sdk.KindStruct,
	)
	assert.NoError(t, err)

	err = proc.Algorithm(
		"NoneAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		window,
		noneAlgo,
		sdk.KindNone,
		sdk.WithDependencies([]sdk.Dependency{{
			Algorithm: structAlgo,
		},
		}),
	)
	assert.NoError(t, err)

	// reregister
	err = proc.Algorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		window,
		noneAlgo,
		sdk.KindNone,
	)

	var targetErr sdk.InvalidAlgorithmArgumentError
	assert.ErrorAs(t, err, &targetErr)

	err = proc.Register(context.Background())
	assert.Error(t, err)
}

func TestDependencyWithLoobackWorks(t *testing.T) {

	proc := sdk.NewProcessor("test_processor", sdk.WithOrcaCore(orcaConnStr))
	window := sdk.WindowType{
		Name:        "MyTriggeringWindow",
		Version:     "1.0.0",
		Description: "A simple triggering window for testing purposes",
		MetadataFields: []sdk.MetadataField{
			{Name: "asset_id", Description: "id of the asset"},
			{Name: "cycles", Description: "an arbitrary field"},
		},
	}

	err := proc.Algorithm(
		"NoneAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		window,
		noneAlgo, sdk.KindNone,
	)

	assert.NoError(t, err)

	err = proc.Algorithm(
		"StructAlgorithm",
		"1.0.0",
		"This algorithm produces a dummy struct",
		window,
		structAlgo,
		sdk.KindStruct,
		sdk.WithDependencies([]sdk.Dependency{{Algorithm: noneAlgo}}),
	)
	assert.NoError(t, err)

	err = proc.Register(context.Background())
	assert.NoError(t, err)

}
