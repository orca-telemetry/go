package orca

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	contract "github.com/orca-telemetry/contract/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Window represents a triggering window
type Window struct {
	TimeFrom time.Time
	TimeTo   time.Time
	Name     string
	Version  string
	Origin   string
	Metadata map[string]any
}

func (w Window) MarshalZerologObject(e *zerolog.Event) {
	e.Time("time_from", w.TimeFrom).
		Time("time_to", w.TimeTo).
		Str("name", w.Name).
		Str("version", w.Version).
		Str("origin", w.Origin).
		Any("metadata", w.Metadata)
}

func NewWindow(
	windowType WindowType,
	timeFrom time.Time,
	timeTo time.Time,
	origin string,
	metadata map[string]any,
) (*Window, error) {
	var errors []error
	for _, m := range windowType.MetadataFields {
		if _, ok := metadata[m.Name]; !ok {
			errors = append(errors, InvalidWindowArgumentError{fmt.Sprintf("required metadata field `%v` not present in metadata", m.Name)})
		}
	}
	if len(errors) > 0 {
		return nil, CompressedError{errors: errors}
	}

	window := &Window{
		TimeFrom: timeFrom,
		TimeTo:   timeTo,
		Name:     windowType.Name,
		Version:  windowType.Version,
		Origin:   origin,
		Metadata: metadata,
	}
	return window, nil
}

// EmitWindow emits a window to Orca-core
func EmitWindow(ctx context.Context, window *Window, orcaCore string, isProduction bool) error {
	log.Info().Object("window", window).Msg("emitting window")

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
