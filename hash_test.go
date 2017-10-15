package solr

import (
	"testing"
)

func TestHash(t *testing.T) {
	t.Run("Test Hash with Doc ID", func(t *testing.T) {
		expected := int32(-1530595841)
		c := CompositeKey{
			ShardKey: "foobar",
			DocID:    "123",
			Bits:     16,
		}
		hash := Hash(c)
		if hash != expected {
			t.Errorf("Error in TestHash. Expected %d but got %d", expected, hash)
		}
	})

	t.Run("Test Hash without Doc ID", func(t *testing.T) {
		expected := int32(-1530604355)
		c := CompositeKey{
			ShardKey: "foobar",
			Bits:     16,
		}
		hash := Hash(c)
		if hash != expected {
			t.Errorf("Error in TestHash. Expected %d but got %d", expected, hash)
		}
	})
}
