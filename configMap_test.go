package kafka

import (
	"reflect"
	"testing"
)

func Test_ConfigMap_copy(t *testing.T) {
	// ARRANGE
	ogcm := configMap{"item": "value"}

	// ACT
	copy := ogcm.copy()

	// ASERT
	t.Run("returns a copy", func(t *testing.T) {
		wanted := reflect.ValueOf(ogcm).UnsafePointer()
		got := reflect.ValueOf(copy).UnsafePointer()
		if wanted == got {
			t.Error("got original, wanted a copy")
		}
	})

	t.Run("returns an exact copy", func(t *testing.T) {
		if !reflect.DeepEqual(copy, ogcm) {
			t.Errorf("wanted %q, got %q", ogcm, copy)
		}
	})
}
