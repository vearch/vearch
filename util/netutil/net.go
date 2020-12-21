package netutil

import (
	"errors"
	"net"
	"net/http"
	"strings"
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

func GetLocalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			ip := getIPFromAddr(addr)
			if ip == nil {
				continue
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("connected to the network?")
}

func getIPFromAddr(addr net.Addr) net.IP {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip == nil || ip.IsLoopback() {
		return nil
	}
	ip = ip.To4()
	if ip == nil {
		return nil // not an ipv4 address
	}

	return ip
}

// ClientIP : get real IP
func ClientIP(r *http.Request) string {
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	ip := strings.TrimSpace(strings.Split(xForwardedFor, ",")[0])
	if ip != "" {
		return ip
	}

	ip = strings.TrimSpace(r.Header.Get("X-Real-Ip"))
	if ip != "" {
		return ip
	}

	if ip, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		return ip
	}

	return ""
}