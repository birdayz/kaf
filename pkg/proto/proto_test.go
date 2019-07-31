package proto

import (
	"testing"
)

func TestBla(t *testing.T) {
	r, err := NewDescriptorRegistry([]string{"/home/j0e/projects/esp-iot/proto", "/home/j0e/projects/esp-iot/proto/3rd_party"}, []string{"3rd_party", "devicetwin/_internal"})
	if err != nil {
		t.Fatal(err)
	}
	_ = r
}
