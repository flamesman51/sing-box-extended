package vpn

import (
	"encoding/binary"
	"io"
	"net/netip"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/gofrs/uuid/v5"
)

const (
	Version = 0
)

const (
	CommandInbound = 1
	CommandTCP     = 2
)

type IPv4 [4]byte

var Destination = M.Socksaddr{
	Fqdn: "sp.vpn.sing-box.arpa",
	Port: 444,
}

var Loopback = netip.AddrFrom4([4]byte{127, 0, 0, 1})

var AddressSerializer = M.NewSerializer(
	M.AddressFamilyByte(0x01, M.AddressFamilyIPv4),
	M.AddressFamilyByte(0x03, M.AddressFamilyIPv6),
	M.AddressFamilyByte(0x02, M.AddressFamilyFqdn),
	M.PortThenAddress(),
)

type ClientRequest struct {
	Key         uuid.UUID
	Command     byte
	Gateway     IPv4
	Destination M.Socksaddr
}

func ReadClientRequest(reader io.Reader) (*ClientRequest, error) {
	var request ClientRequest
	var version uint8
	err := binary.Read(reader, binary.BigEndian, &version)
	if err != nil {
		return nil, err
	}
	if version != Version {
		return nil, E.New("unknown version: ", version)
	}
	_, err = io.ReadFull(reader, request.Key[:])
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, binary.BigEndian, &request.Command)
	if err != nil {
		return nil, err
	}
	_, err = io.ReadFull(reader, request.Gateway[:])
	if err != nil {
		return nil, err
	}
	request.Destination, err = AddressSerializer.ReadAddrPort(reader)
	if err != nil {
		return nil, err
	}
	return &request, nil
}

func WriteClientRequest(writer io.Writer, request *ClientRequest) error {
	var requestLen int
	requestLen += 1  // version
	requestLen += 16 // key
	requestLen += 1  // command
	requestLen += 4  // gateway
	requestLen += AddressSerializer.AddrPortLen(request.Destination)
	buffer := buf.NewSize(requestLen)
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(Version),
		common.Error(buffer.Write(request.Key[:])),
		buffer.WriteByte(request.Command),
		common.Error(buffer.Write(request.Gateway[:])),
		AddressSerializer.WriteAddrPort(buffer, request.Destination),
	)
	return common.Error(writer.Write(buffer.Bytes()))
}

type ServerRequest struct {
	Source      IPv4
	Destination M.Socksaddr
}

func ReadServerRequest(reader io.Reader) (*ServerRequest, error) {
	var request ServerRequest
	_, err := io.ReadFull(reader, request.Source[:])
	if err != nil {
		return nil, err
	}
	request.Destination, err = AddressSerializer.ReadAddrPort(reader)
	if err != nil {
		return nil, err
	}
	return &request, nil
}

func WriteServerRequest(writer io.Writer, request *ServerRequest) error {
	var requestLen int
	requestLen += 4 // source
	requestLen += AddressSerializer.AddrPortLen(request.Destination)
	buffer := buf.NewSize(requestLen)
	defer buffer.Release()
	common.Must(
		common.Error(buffer.Write(request.Source[:])),
		AddressSerializer.WriteAddrPort(buffer, request.Destination),
	)
	return common.Error(writer.Write(buffer.Bytes()))
}
