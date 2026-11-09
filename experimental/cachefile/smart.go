package cachefile

import (
	"bytes"
	"errors"
	"os"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
)

const smartHistoryMaxBytes = 1 << 20

var (
	bucketSmartHistory  = []byte("smart_history")
	bucketSmartMetadata = []byte("smart_metadata")
	keySmartMetadata    = []byte("installation")
)

func (c *CacheFile) LoadSmartHistory(group string) (*adapter.SmartHistory, error) {
	var history *adapter.SmartHistory
	err := c.view(func(tx *bbolt.Tx) error {
		bucket := c.bucket(tx, bucketSmartHistory)
		if bucket == nil {
			return os.ErrNotExist
		}
		data := bucket.Get([]byte(group))
		if data == nil {
			return os.ErrNotExist
		}
		if len(data) > smartHistoryMaxBytes {
			return errors.New("smart history exceeds 1 MiB")
		}
		history = &adapter.SmartHistory{Data: bytes.Clone(data)}
		return nil
	})
	return history, err
}

func (c *CacheFile) SaveSmartHistory(group string, history *adapter.SmartHistory) error {
	if history == nil {
		return os.ErrInvalid
	}
	if len(history.Data) > smartHistoryMaxBytes {
		return errors.New("smart history exceeds 1 MiB")
	}
	return c.update(func(tx *bbolt.Tx) error {
		bucket, err := c.createBucket(tx, bucketSmartHistory)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(group), history.Data)
	})
}

func (c *CacheFile) ResetSmartHistory(group string) error {
	return c.update(func(tx *bbolt.Tx) error {
		bucket := c.bucket(tx, bucketSmartHistory)
		if bucket == nil {
			return nil
		}
		return bucket.Delete([]byte(group))
	})
}

func (c *CacheFile) LoadSmartMetadata() (*adapter.SmartMetadata, error) {
	var metadata *adapter.SmartMetadata
	err := c.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartMetadata)
		if bucket == nil {
			return os.ErrNotExist
		}
		data := bucket.Get(keySmartMetadata)
		if data == nil {
			return os.ErrNotExist
		}
		metadata = &adapter.SmartMetadata{Data: bytes.Clone(data)}
		return nil
	})
	return metadata, err
}

func (c *CacheFile) SaveSmartMetadata(metadata *adapter.SmartMetadata) error {
	if metadata == nil {
		return os.ErrInvalid
	}
	return c.update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketSmartMetadata)
		if err != nil {
			return err
		}
		return bucket.Put(keySmartMetadata, metadata.Data)
	})
}
