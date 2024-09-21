package settings

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Config struct {
	Version string
	Remotes
}

func DecodeConfig(b []byte) (*Config, error) {
	c := &Config{}
	err := json.Unmarshal(b, c)
	if err != nil {
		return nil, fmt.Errorf("Invalid JSON config")
	}
	return c, nil
}

func EncodeConfig(c Config) []byte {
	//Config doesn't have types that can fail to marshal
	b, _ := json.Marshal(c)
	return b
}

func ParseAuth(auth string) (string, string) {
	if strings.Contains(auth, ":") {
		pair := strings.SplitN(auth, ":", 2)
		return pair[0], pair[1]
	}
	return "", ""
}
