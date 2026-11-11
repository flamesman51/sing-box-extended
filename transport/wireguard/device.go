package wireguard

import (
	"context"
	"net/netip"
	"time"

	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/control"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/wireguard-go/device"
	wgTun "github.com/sagernet/wireguard-go/tun"
)

type Device interface {
	wgTun.Device
	N.Dialer
	Start() error
	SetDevice(device *device.Device)
	Inet4Address() netip.Addr
	Inet6Address() netip.Addr
}

type DeviceOptions struct {
	Context         context.Context
	Logger          logger.ContextLogger
	System          bool
	Handler         tun.Handler
	UDPTimeout      time.Duration
	ICMPTimeout     time.Duration
	UDPMapping      tun.NATMapping
	UDPFiltering    tun.NATFiltering
	UDPNATMax       uint32
	NetworkMonitor  tun.NetworkUpdateMonitor
	InterfaceFinder control.InterfaceFinder
	CreateDialer    func(interfaceName string) N.Dialer
	Name            string
	MTU             uint32
	Address         []netip.Prefix
	AllowedAddress  []netip.Prefix
}

func NewDevice(options DeviceOptions) (Device, error) {
	if !options.System {
		return newStackDevice(options)
	} else if !tun.WithGVisor {
		return newSystemDevice(options)
	} else {
		return newSystemStackDevice(options)
	}
}

func validateDeviceOptions(options DeviceOptions) error {
	if !tun.WithGVisor {
		if !options.System {
			_, err := newStackDevice(options)
			return err
		}
		return nil
	}
	for index, prefix := range options.Address {
		if !prefix.IsValid() {
			return E.New("invalid local address")
		}
		if prefix.Addr() == netip.AddrFrom4([4]byte{255, 255, 255, 255}) {
			return E.New("parse local address ", prefix, ": duplicate address")
		}
		for _, previous := range options.Address[:index] {
			if prefix.Addr() == previous.Addr() {
				return E.New("parse local address ", prefix, ": duplicate address")
			}
		}
	}
	return nil
}
