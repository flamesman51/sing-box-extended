package client

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/common/tls"
	E "github.com/sagernet/sing/common/exceptions"

	"google.golang.org/grpc/credentials"
)

type tlsCreds struct {
	config tls.Config
}

func (c tlsCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
		ServerName:       c.config.ServerName(),
	}
}

func (c *tlsCreds) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, err := tls.ClientHandshake(ctx, rawConn, c.config)
	if err != nil {
		return nil, nil, err
	}
	return conn, credentials.TLSInfo{State: conn.ConnectionState()}, err
}

func (c *tlsCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, E.New("not implemented")
}

func (c *tlsCreds) Clone() credentials.TransportCredentials {
	return &tlsCreds{config: c.config.Clone()}
}

func (c *tlsCreds) OverrideServerName(serverNameOverride string) error {
	c.config.SetServerName(serverNameOverride)
	return nil
}
