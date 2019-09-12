package netutil

import (
	"net"
)

// GetPrivateIP returns a private IP address, or panics if no IP is available.
func GetPrivateIP() net.IP {
	addr := getPrivateIPIfAvailable()
	if addr.IsUnspecified() {
		panic("No private IP address is available")
	}
	return addr
}

func GetPrivateIPByName(name string) net.IP {
	addr := getPrivateIPByName(name)
	if addr.IsUnspecified() {
		panic("No private IP address is available")
	}
	return addr
}

func getPrivateIPIfAvailable() net.IP {
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		default:
			continue
		}

		if ip == nil || ip.To4() == nil { //fix, not support ip v6
			continue
		}

		if !ip.IsLoopback() {
			return ip.To4()
		}
	}
	return net.IPv4zero
}

func getPrivateIPByName(name string) net.IP {
	itfc, err := net.InterfaceByName(name)
	if err != nil || itfc == nil {
		return nil
	}
	addrs, _ := itfc.Addrs()
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		default:
			continue
		}

		if ip == nil || ip.To4() == nil { //fix, not support ip v6
			continue
		}

		if !ip.IsLoopback() {
			return ip.To4()
		}
	}
	return net.IPv4zero
}
