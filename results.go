package orca

// Result types
type (
	// StructResult represents a structured result
	StructResult struct {
		Value map[string]any
	}

	// ValueResult represents a single numeric or boolean value
	ValueResult struct {
		Value float32
	}

	// ArrayResult represents an array of numeric or boolean values
	ArrayResult struct {
		Value []float32
	}

	// NoneResult represents no result
	NoneResult struct{}
)

func (StructResult) isResult() {}
func (ValueResult) isResult()  {}
func (ArrayResult) isResult()  {}
func (NoneResult) isResult()   {}

// signature alone is not enough to determine the concrete return type.
// that is why we have this `Kind` pattern.
type ResultKind int

const (
	KindStruct ResultKind = iota
	KindValue
	KindArray
	KindNone
)

func (StructResult) Kind() ResultKind { return KindStruct }
func (ValueResult) Kind() ResultKind  { return KindValue }
func (ArrayResult) Kind() ResultKind  { return KindArray }
func (NoneResult) Kind() ResultKind   { return KindNone }

// Result is the interface that all result types implement
type Result interface {
	isResult()
	Kind() ResultKind
}
