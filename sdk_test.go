package orca_test

import (
	"context"
	"testing"

	sdk "github.com/orca-telemetry/golang-sdk"
	"github.com/stretchr/testify/assert"
)

// define some algorithms
func _noneAlgo(params *sdk.ExecutionParams) (sdk.Result, error) {
	return sdk.NoneResult{}, nil
}

func TestAlgorithmsRegistered(t *testing.T) {

	// define the processor
	proc := sdk.NewProcessor("test_processor", sdk.WithOrcaCore(orcaConnStr))

	err := proc.Algorithm(
		"TestAlgorithm",
		"1.0.0",
		"This algorithm does nothing",
		sdk.WindowType{
			Name:        "MyTriggeringWindow",
			Version:     "1.0.0",
			Description: "A simple triggering window for testing purposes",
			MetadataFields: []sdk.MetadataField{
				{Name: "asset_id", Description: "id of the asset"},
				{Name: "cycles", Description: "an arbitrary field"},
			},
		},
		_noneAlgo,
	)
	assert.NoError(t, err)

	proc.Register(context.Background())
}
