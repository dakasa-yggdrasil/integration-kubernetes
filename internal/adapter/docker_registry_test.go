package adapter

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestDockerConfigJSONBase64(t *testing.T) {
	payload, err := BuildDockerConfigJSON("ghcr.io", "alice", "secret")
	if err != nil {
		t.Fatalf("BuildDockerConfigJSON error: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	auths, _ := parsed["auths"].(map[string]any)
	entry, _ := auths["ghcr.io"].(map[string]any)
	authB64, _ := entry["auth"].(string)
	decoded, err := base64.StdEncoding.DecodeString(authB64)
	if err != nil {
		t.Fatalf("auth is not base64: %v", err)
	}
	if string(decoded) != "alice:secret" {
		t.Errorf("decoded auth = %q, want alice:secret", decoded)
	}
}

func TestBuildDockerConfigJSONRejectsEmpty(t *testing.T) {
	if _, err := BuildDockerConfigJSON("", "alice", "secret"); err == nil {
		t.Error("empty registry should fail")
	}
	if _, err := BuildDockerConfigJSON("ghcr.io", "", "secret"); err == nil {
		t.Error("empty username should fail")
	}
	if _, err := BuildDockerConfigJSON("ghcr.io", "alice", ""); err == nil {
		t.Error("empty password should fail")
	}
}
