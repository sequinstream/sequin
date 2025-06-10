package testutil

import (
	"reflect"
	"testing"

	"github.com/goccy/go-yaml"
)

// CompareYAML parses two YAML strings and compares their semantic meaning
func CompareYAML(t *testing.T, got, want string) {
	var gotObj, wantObj interface{}
	if err := yaml.Unmarshal([]byte(got), &gotObj); err != nil {
		t.Errorf("Failed to parse got YAML: %v", err)
		return
	}
	if err := yaml.Unmarshal([]byte(want), &wantObj); err != nil {
		t.Errorf("Failed to parse want YAML: %v", err)
		return
	}
	if !reflect.DeepEqual(gotObj, wantObj) {
		t.Errorf("YAML objects do not match.\nGot:\n%s\nWant:\n%s", got, want)
	}
}
