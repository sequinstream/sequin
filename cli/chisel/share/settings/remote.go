package settings

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// short-hand conversions (see remote_test)
//   3000 ->
//     local  127.0.0.1:3000
//     remote 127.0.0.1:3000
//   foobar.com:3000 ->
//     local  127.0.0.1:3000
//     remote foobar.com:3000
//   3000:google.com:80 ->
//     local  127.0.0.1:3000
//     remote google.com:80
//   192.168.0.1:3000:google.com:80 ->
//     local  192.168.0.1:3000
//     remote google.com:80
//   127.0.0.1:1080:socks
//     local  127.0.0.1:1080
//     remote socks
//   stdio:example.com:22
//     local  stdio
//     remote example.com:22
//   1.1.1.1:53/udp
//     local  127.0.0.1:53/udp
//     remote 1.1.1.1:53/udp

type Remote struct {
	LocalHost, LocalPort, LocalProto    string
	RemoteHost, RemotePort, RemoteProto string
	Socks, Reverse, Stdio               bool
}

const revPrefix = "R:"

func DecodeRemote(s string) (*Remote, error) {
	if !strings.HasPrefix(s, revPrefix) {
		return nil, errors.New("Remote string must start with 'R:'")
	}
	s = strings.TrimPrefix(s, revPrefix)
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid remote format. Expected 'R:<localPort>:<localHost>:<remoteHostOrId>'")
	}
	localPort := parts[0]
	localHost := parts[1]
	remoteHost := parts[2]

	if localPort == "" || localHost == "" || remoteHost == "" {
		return nil, errors.New("invalid remote: missing fields")
	}

	// Because it's reversed, we need to swap the local and remote
	r := &Remote{
		LocalHost:   remoteHost,
		LocalPort:   "",
		RemoteHost:  localHost,
		RemotePort:  localPort,
		Reverse:     true,
		LocalProto:  "tcp",
		RemoteProto: "tcp",
	}
	return r, nil
}

func isPort(s string) bool {
	n, err := strconv.Atoi(s)
	if err != nil {
		return false
	}
	if n <= 0 || n > 65535 {
		return false
	}
	return true
}

// isHost checks if the given string is a valid host.
// Modified to accept any non-empty string, as we're changing this interface.
func isHost(s string) bool {
	// _, err := url.Parse("//" + s)
	// if err != nil {
	//     return false
	// }
	// return true
	return s != ""
}

var l4Proto = regexp.MustCompile(`(?i)\/(tcp|udp)$`)

// L4Proto extacts the layer-4 protocol from the given string
func L4Proto(s string) (head, proto string) {
	if l4Proto.MatchString(s) {
		l := len(s)
		return strings.ToLower(s[:l-4]), s[l-3:]
	}
	return s, ""
}

// implement Stringer
func (r Remote) String() string {
	sb := strings.Builder{}
	if r.Reverse {
		sb.WriteString(revPrefix)
	}
	sb.WriteString(strings.TrimPrefix(r.Local(), "0.0.0.0:"))
	sb.WriteString("=>")
	sb.WriteString(strings.TrimPrefix(r.Remote(), "127.0.0.1:"))
	if r.RemoteProto == "udp" {
		sb.WriteString("/udp")
	}
	return sb.String()
}

// Encode remote to a string
func (r Remote) Encode() string {
	if r.LocalPort == "" {
		r.LocalPort = r.RemotePort
	}
	local := r.Local()
	remote := r.Remote()
	if r.RemoteProto == "udp" {
		remote += "/udp"
	}
	if r.Reverse {
		return "R:" + local + ":" + remote
	}
	return local + ":" + remote
}

// Local is the decodable local portion
func (r Remote) Local() string {
	if r.Stdio {
		return "stdio"
	}
	if r.LocalHost == "" {
		r.LocalHost = "0.0.0.0"
	}
	return r.LocalHost + ":" + r.LocalPort
}

// Remote is the decodable remote portion
func (r Remote) Remote() string {
	if r.Socks {
		return "socks"
	}
	if r.RemoteHost == "" {
		r.RemoteHost = "127.0.0.1"
	}
	return r.RemoteHost + ":" + r.RemotePort
}

// CanListen checks if the port can be listened on
func (r Remote) CanListen() bool {
	//valid protocols
	switch r.LocalProto {
	case "tcp":
		conn, err := net.Listen("tcp", r.Local())
		if err == nil {
			conn.Close()
			return true
		}
		return false
	case "udp":
		addr, err := net.ResolveUDPAddr("udp", r.Local())
		if err != nil {
			return false
		}
		conn, err := net.ListenUDP(r.LocalProto, addr)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}
	//invalid
	return false
}

type Remotes []*Remote

// Filter out forward reversed/non-reversed remotes
func (rs Remotes) Reversed(reverse bool) Remotes {
	subset := Remotes{}
	for _, r := range rs {
		match := r.Reverse == reverse
		if match {
			subset = append(subset, r)
		}
	}
	return subset
}

// Encode back into strings
func (rs Remotes) Encode() []string {
	s := make([]string, len(rs))
	for i, r := range rs {
		s[i] = r.Encode()
	}
	return s
}
