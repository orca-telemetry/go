package orca

import "fmt"

type (
	InvalidDependencyError          struct{ msg string }
	InvalidWindowArgumentError      struct{ msg string }
	InvalidAlgorithmArgumentError   struct{ msg string }
	BrokenRemoteAlgorithmStubsError struct{ msg string }
	InvalidAlgorithmReturnTypeError struct{ msg string }
	InvalidMetadataFieldError       struct{ msg string }
)

func (e InvalidDependencyError) Error() string {
	return fmt.Sprintf("bad metadata field - %v", e.msg)
}
func (e InvalidWindowArgumentError) Error() string {
	return fmt.Sprintf("bad window argument - %v", e.msg)
}
func (e InvalidAlgorithmArgumentError) Error() string {
	return fmt.Sprintf("bad algorithm argument - %v", e.msg)
}
func (e BrokenRemoteAlgorithmStubsError) Error() string {
	return fmt.Sprintf("broken remote algorithm stub, rerun `orca sync` - %v", e.msg)
}
func (e InvalidAlgorithmReturnTypeError) Error() string {
	return fmt.Sprintf("invalid algorithm return type - %v", e.msg)
}
func (e InvalidMetadataFieldError) Error() string {
	return fmt.Sprintf("invalid metadata field - %v", e.msg)
}
