package adapter

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// BuildDockerConfigJSON emits the byte payload that Kubernetes expects
// in a `.dockerconfigjson`-typed Secret. The format is the same as
// `~/.docker/config.json`: one object per registry with a base64
// `auth` of `user:password` (no colons allowed in the username).
//
// The caller is responsible for persisting the bytes into a Secret
// keyed `.dockerconfigjson`; this helper does no Kubernetes I/O.
func BuildDockerConfigJSON(registry, username, password string) ([]byte, error) {
	registry = strings.TrimSpace(registry)
	username = strings.TrimSpace(username)
	password = strings.TrimSpace(password)
	if registry == "" {
		return nil, fmt.Errorf("registry is required")
	}
	if username == "" {
		return nil, fmt.Errorf("username is required")
	}
	if password == "" {
		return nil, fmt.Errorf("password is required")
	}
	if strings.ContainsRune(username, ':') {
		return nil, fmt.Errorf("username must not contain ':'")
	}
	auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	payload := map[string]any{
		"auths": map[string]any{
			registry: map[string]any{
				"username": username,
				"password": password,
				"auth":     auth,
			},
		},
	}
	return json.Marshal(payload)
}
