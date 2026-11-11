package failover

import (
	"encoding/binary"
	"io"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/gofrs/uuid/v5"
)

const (
	Version    = 0
	BufferSize = 10
)

const (
	CommandTCP       = 1
	CommandReconnect = 2
)

const (
	StatusOK uint8 = iota + 1
	StatusSessionNotFound
)

var (
	SessionClosed   = E.New("session closed")
	SessionNotFound = E.New("session not found")
	SessionExpired  = E.New("session expired")
	SessionBroken   = E.New("session broken")
)

var Destination = M.Socksaddr{
	Fqdn: "sp.failover.sing-box.arpa",
	Port: 444,
}

var AddressSerializer = M.NewSerializer(
	M.AddressFamilyByte(0x01, M.AddressFamilyIPv4),
	M.AddressFamilyByte(0x03, M.AddressFamilyIPv6),
	M.AddressFamilyByte(0x02, M.AddressFamilyFqdn),
	M.PortThenAddress(),
)

type Request struct {
	UUID        uuid.UUID
	Command     byte
	Destination M.Socksaddr
}

func ReadRequest(reader io.Reader) (*Request, error) {
	var request Request
	var version uint8
	err := binary.Read(reader, binary.BigEndian, &version)
	if err != nil {
		return nil, err
	}
	if version != Version {
		return nil, E.New("unknown version: ", version)
	}
	_, err = io.ReadFull(reader, request.UUID[:])
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, binary.BigEndian, &request.Command)
	if err != nil {
		return nil, err
	}
	request.Destination, err = AddressSerializer.ReadAddrPort(reader)
	if err != nil {
		return nil, err
	}
	return &request, nil
}

func WriteRequest(writer io.Writer, request *Request) error {
	var requestLen int
	requestLen += 1  // version
	requestLen += 1  // command
	requestLen += 16 // UUID
	requestLen += AddressSerializer.AddrPortLen(request.Destination)
	buffer := buf.NewSize(requestLen)
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(Version),
		common.Error(buffer.Write(request.UUID[:])),
		buffer.WriteByte(request.Command),
	)
	err := AddressSerializer.WriteAddrPort(buffer, request.Destination)
	if err != nil {
		return err
	}
	return common.Error(writer.Write(buffer.Bytes()))
}
