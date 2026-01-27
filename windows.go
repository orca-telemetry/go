package orca

import (
	"time"

	"github.com/rs/zerolog"
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
