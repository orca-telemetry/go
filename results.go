package orca

// Value types for ValueResult
type (
	Float64Value float64
	IntValue     int
	BoolValue    bool
)

// isValueResult is a sealed interface for ValueResult value types
type isValueResult interface {
	isValueResult()
}

func (Float64Value) isValueResult() {}
func (IntValue) isValueResult()     {}
func (BoolValue) isValueResult()    {}

// Array element types for ArrayResult
type (
	Float64Array []float64
	IntArray     []int
)

// isArrayResult is a sealed interface for ArrayResult value types
type isArrayResult interface {
	isArrayResult()
}

func (Float64Array) isArrayResult() {}
func (IntArray) isArrayResult()     {}

// Result types
type (
	// StructResult represents a structured result
	StructResult struct {
		Value map[string]any
	}

	// ValueResult represents a single numeric or boolean value
	ValueResult struct {
		Value isValueResult
	}

	// ArrayResult represents an array of numeric or boolean values
	ArrayResult struct {
		Value isArrayResult
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
