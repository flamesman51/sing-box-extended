//go:build with_gvisor && with_wireguard

package libbox

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
)

func TestGoroutineCheckConfigLab(t *testing.T) {
	config := goroutineLabConfig(t)
	failedConfig := strings.Replace(config, `"outbounds":[`, `"outbounds":[{"type":"unknown-lab-outbound"},`, 1)
	baseline := runtime.NumGoroutine()
	for range 20 {
		if err := CheckConfig(config); err != nil {
			t.Fatal(err)
		}
		if err := CheckConfig(failedConfig); err == nil {
			t.Fatal("invalid configuration passed validation")
		}
	}
	if directory := os.Getenv("GOROUTINE_LAB_OUTPUT"); directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		file, err := os.Create(filepath.Join(directory, "validation.txt"))
		if err != nil {
			t.Fatal(err)
		}
		writeErr := pprof.Lookup("goroutine").WriteTo(file, 1)
		closeErr := file.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	count := runtime.NumGoroutine()
	t.Logf("validation goroutines: before=%d after=%d", baseline, count)
	if count > baseline+5 {
		t.Fatalf("validation leaks goroutines: before=%d after=%d", baseline, count)
	}
}
