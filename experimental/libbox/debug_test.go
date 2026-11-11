package libbox_test

import (
	"strings"
	"testing"

	"github.com/sagernet/sing-box/experimental/libbox"
)

func TestGoroutineDump(t *testing.T) {
	ready := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go waitForGoroutineDump(ready, release, done)
	defer func() {
		close(release)
		<-done
	}()
	<-ready

	dump := libbox.GoroutineDump()
	for _, want := range []string{"goroutine ", "libbox.GoroutineDump", "libbox_test.TestGoroutineDump", "libbox_test.waitForGoroutineDump"} {
		if !strings.Contains(dump, want) {
			t.Errorf("goroutine dump does not contain %q", want)
		}
	}
}

func waitForGoroutineDump(ready chan<- struct{}, release <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	close(ready)
	<-release
}
