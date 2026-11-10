package wireguard

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/wireguard-go/conn"
)

type recordingLogger struct {
	logger.Logger
	debug []string
	error []string
}

func (l *recordingLogger) Debug(args ...any) { l.debug = append(l.debug, fmt.Sprint(args...)) }
func (l *recordingLogger) Error(args ...any) { l.error = append(l.error, fmt.Sprint(args...)) }

func TestLogDeviceErrorGSOFallback(t *testing.T) {
	const format = "%v - Failed to send handshake initiation: %v"

	var sent recordingLogger
	logDeviceError(&sent, format, "peer", conn.ErrUDPGSODisabled{})
	if len(sent.error) != 0 || len(sent.debug) != 1 {
		t.Fatalf("successful resend: error=%q debug=%q", sent.error, sent.debug)
	}

	var failed recordingLogger
	logDeviceError(&failed, format, "peer", conn.ErrUDPGSODisabled{RetryErr: syscall.ENETUNREACH})
	if len(failed.error) != 1 || !strings.HasSuffix(failed.error[0], syscall.ENETUNREACH.Error()) {
		t.Fatalf("failed resend: error=%q", failed.error)
	}

	var other recordingLogger
	logDeviceError(&other, format, "peer", errors.New("boom"))
	if len(other.debug) != 0 || len(other.error) != 1 || other.error[0] != "peer - failed to send handshake initiation: boom" {
		t.Fatalf("other error: error=%q debug=%q", other.error, other.debug)
	}
}
