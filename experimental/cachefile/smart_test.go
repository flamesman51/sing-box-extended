package cachefile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
)

func TestSmartCacheScopeRestartAndReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	first := New(context.Background(), nil, option.CacheFileOptions{Path: path, CacheID: "first"})
	if err := first.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.DB.Close() })
	if err := first.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("first-history")}); err != nil {
		t.Fatal(err)
	}
	if err := first.SaveSmartMetadata(&adapter.SmartMetadata{Data: []byte("installation-and-budget")}); err != nil {
		t.Fatal(err)
	}
	second := New(context.Background(), nil, option.CacheFileOptions{Path: path, CacheID: "second"})
	second.DB = first.DB
	if _, err := second.LoadSmartHistory("auto"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign profile history error = %v", err)
	}
	metadata, err := second.LoadSmartMetadata()
	if err != nil || string(metadata.Data) != "installation-and-budget" {
		t.Fatalf("installation metadata = %v, %v", metadata, err)
	}
	if err = second.SaveSmartHistory("auto", &adapter.SmartHistory{Data: []byte("second-history")}); err != nil {
		t.Fatal(err)
	}
	if err = first.DB.Close(); err != nil {
		t.Fatal(err)
	}
	if err = first.start(); err != nil {
		t.Fatal(err)
	}
	second.DB = first.DB
	for _, item := range []struct {
		cache *CacheFile
		want  string
	}{{first, "first-history"}, {second, "second-history"}} {
		history, loadErr := item.cache.LoadSmartHistory("auto")
		if loadErr != nil || string(history.Data) != item.want {
			t.Fatalf("restarted history = %v, %v; want %q", history, loadErr, item.want)
		}
	}
	if err = first.ResetSmartHistory("auto"); err != nil {
		t.Fatal(err)
	}
	if _, err = first.LoadSmartHistory("auto"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reset history error = %v", err)
	}
	metadata, err = first.LoadSmartMetadata()
	if err != nil || string(metadata.Data) != "installation-and-budget" {
		t.Fatalf("reset changed metadata = %v, %v", metadata, err)
	}
	if history, loadErr := second.LoadSmartHistory("auto"); loadErr != nil || string(history.Data) != "second-history" {
		t.Fatalf("reset crossed cache ID: %v, %v", history, loadErr)
	}
}

func TestSmartCacheSizeAndDetachedReads(t *testing.T) {
	cache := New(context.Background(), nil, option.CacheFileOptions{Path: filepath.Join(t.TempDir(), "cache.db")})
	if err := cache.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.DB.Close() })
	data := bytes.Repeat([]byte("a"), smartHistoryMaxBytes)
	if err := cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: data}); err != nil {
		t.Fatal(err)
	}
	loaded, err := cache.LoadSmartHistory("auto")
	if err != nil {
		t.Fatal(err)
	}
	loaded.Data[0] = 'b'
	if err = cache.SaveSmartHistory("auto", &adapter.SmartHistory{Data: append(data, 'c')}); err == nil {
		t.Fatal("oversized history was accepted")
	}
	reloaded, err := cache.LoadSmartHistory("auto")
	if err != nil || !bytes.Equal(reloaded.Data, data) {
		t.Fatalf("failed save or read mutation changed persisted history: %v", err)
	}
}
