package adapter

import (
	"bytes"
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"golang.org/x/crypto/ssh"
)

// SmartHistory is an engine-owned history document scoped to one cache ID and group.
type SmartHistory struct {
	Data []byte
}

// SmartMetadata is an engine-owned, installation-wide metadata document.
type SmartMetadata struct {
	Data []byte
}

// SmartIdentityProvider identifies the actual configured outbound instance, not its display tag.
type SmartIdentityProvider interface {
	SmartIdentity(outbound Outbound) (local, server string, err error)
}

// ComputeSmartIdentity derives stable fingerprints without retaining configured credentials.
func ComputeSmartIdentity(kind string, options any) (local, server string, err error) {
	data, err := json.Marshal(options)
	if err != nil {
		return "", "", errors.New("cannot encode smart identity options")
	}
	var configured map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err = decoder.Decode(&configured); err != nil || configured == nil {
		return "", "", errors.New("invalid smart identity options")
	}
	if err = smartSanitizeIdentity(configured); err != nil {
		return "", "", err
	}
	serverOptions := make(map[string]any)
	for _, key := range []string{"server", "server_port", "server_ports", "public_key", "peer_public_key", "host_key", "host_key_algorithms"} {
		if value, exists := configured[key]; exists {
			serverOptions[key] = value
		}
	}
	if peers, ok := configured["peers"].([]any); ok {
		serverPeers := make([]map[string]any, 0, len(peers))
		for _, value := range peers {
			if peer, ok := value.(map[string]any); ok {
				serverPeer := make(map[string]any)
				for _, key := range []string{"address", "port", "public_key"} {
					if value, exists := peer[key]; exists {
						serverPeer[key] = value
					}
				}
				serverPeers = append(serverPeers, serverPeer)
			}
		}
		serverOptions["peers"] = serverPeers
	}
	if tlsOptions, ok := configured["tls"].(map[string]any); ok {
		serverTLS := make(map[string]any)
		for _, key := range []string{"server_name", "certificate_public_key_sha256"} {
			if value, exists := tlsOptions[key]; exists {
				serverTLS[key] = value
			}
		}
		if reality, ok := tlsOptions["reality"].(map[string]any); ok {
			serverTLS["reality_public_key"] = reality["public_key"]
		}
		serverOptions["tls"] = serverTLS
	}
	serverDocument := struct {
		Type   string         `json:"type"`
		Server map[string]any `json:"server"`
	}{kind, serverOptions}
	data, err = json.Marshal(serverDocument)
	if err != nil {
		return "", "", errors.New("cannot encode smart server identity")
	}
	serverHash := sha256.Sum256(data)
	server = hex.EncodeToString(serverHash[:])
	localDocument := struct {
		Server  string         `json:"server_identity"`
		Options map[string]any `json:"options"`
	}{server, configured}
	data, err = json.Marshal(localDocument)
	if err != nil {
		return "", "", errors.New("cannot encode smart local identity")
	}
	localHash := sha256.Sum256(data)
	return hex.EncodeToString(localHash[:]), server, nil
}

func smartSanitizeIdentity(configured map[string]any) error {
	for _, field := range []string{"private_key", "client_key", "key"} {
		value := smartIdentityKeyText(configured[field])
		path, _ := configured[field+"_path"].(string)
		if field == "key" && path == "" && !strings.Contains(value, "PRIVATE KEY") {
			continue
		}
		if value == "" && path != "" {
			content, err := os.ReadFile(os.ExpandEnv(path))
			if err != nil {
				return errors.New("cannot read smart identity private key")
			}
			value = string(content)
		}
		if field == "key" && !strings.Contains(value, "PRIVATE KEY") {
			sum := sha256.Sum256([]byte(value))
			configured[field] = hex.EncodeToString(sum[:])
			delete(configured, field+"_path")
			continue
		}
		delete(configured, field)
		delete(configured, field+"_path")
		passphrase, _ := configured[field+"_passphrase"].(string)
		delete(configured, field+"_passphrase")
		if value == "" {
			continue
		}
		public, err := smartIdentityPublicKey(value, passphrase)
		if err != nil {
			return err
		}
		configured["local_public_"+field] = public
	}
	for field, value := range configured {
		switch value := value.(type) {
		case map[string]any:
			if err := smartSanitizeIdentity(value); err != nil {
				return err
			}
		case []any:
			for _, child := range value {
				if object, ok := child.(map[string]any); ok {
					if err := smartSanitizeIdentity(object); err != nil {
						return err
					}
				} else if strings.Contains(smartIdentityKeyText(child), "PRIVATE KEY") {
					return errors.New("unsupported embedded smart identity private key")
				}
			}
		case string:
			if strings.Contains(value, "PRIVATE KEY") {
				return errors.New("unsupported embedded smart identity private key")
			}
		}
		switch field {
		case "uuid", "password", "username", "user", "auth", "token", "key", "pre_shared_key", "auth_str", "auth_string", "auth_token", "license_key", "psk":
			data, err := json.Marshal(value)
			if err != nil {
				return errors.New("cannot encode smart identity credential")
			}
			sum := sha256.Sum256(data)
			configured[field] = hex.EncodeToString(sum[:])
		}
	}
	return nil
}

func smartIdentityKeyText(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case []any:
		lines := make([]string, 0, len(value))
		for _, line := range value {
			text, ok := line.(string)
			if !ok {
				return ""
			}
			lines = append(lines, text)
		}
		return strings.Join(lines, "\n")
	default:
		return ""
	}
}

func smartIdentityPublicKey(value, passphrase string) (string, error) {
	if strings.Contains(value, "PRIVATE KEY") {
		var signer ssh.Signer
		var err error
		if passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(value), []byte(passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey([]byte(value))
		}
		if err != nil {
			return "", errors.New("invalid smart identity private key")
		}
		return base64.StdEncoding.EncodeToString(signer.PublicKey().Marshal()), nil
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(value)
	}
	if err != nil {
		return "", errors.New("invalid smart identity private key")
	}
	private, err := ecdh.X25519().NewPrivateKey(decoded)
	clear(decoded)
	if err != nil {
		return "", errors.New("invalid smart identity private key")
	}
	return base64.StdEncoding.EncodeToString(private.PublicKey().Bytes()), nil
}
