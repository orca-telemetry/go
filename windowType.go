package orca

import "fmt"

// MetadataField describes a metadata field for a window type
type MetadataField struct {
	Name        string
	Description string
}

// Validate validates the metadata field
func (m MetadataField) Validate() error {
	if m.Name == "" {
		return InvalidMetadataFieldError{"metadata field name cannot be empty"}
	}
	if m.Description == "" {
		return InvalidMetadataFieldError{"metadata field description cannot be empty"}
	}
	return nil
}

// WindowType defines a window type that can trigger algorithms
type WindowType struct {
	Name           string
	Version        string
	Description    string
	MetadataFields []MetadataField
}

// Validate validates the window type
func (w WindowType) Validate() error {
	if !windowNamePattern.MatchString(w.Name) {
		return InvalidWindowArgumentError{fmt.Sprintf("window name '%s' must be in PascalCase", w.Name)}
	}
	if !semverPattern.MatchString(w.Version) {
		return InvalidWindowArgumentError{fmt.Sprintf("window version '%s' must follow basic semantic versioning (e.g., '1.0.0')", w.Version)}
	}

	seen := make(map[MetadataField]bool)
	for _, field := range w.MetadataFields {
		if err := field.Validate(); err != nil {
			return err
		}
		if seen[field] {
			return InvalidWindowArgumentError{fmt.Sprintf("duplicate metadata field: name='%s', description='%s'", field.Name, field.Description)}
		}
		seen[field] = true
	}
	return nil
}

// FullName returns the full window name as "name_version"
func (w WindowType) FullName() string {
	return fmt.Sprintf("%s_%s", w.Name, w.Version)
}
