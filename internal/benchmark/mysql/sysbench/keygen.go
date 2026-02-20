package sysbench

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

// PrepareUUIDs generates proper UUIDs using Go libraries and copies the file
// into the container at /tmp/uuids.txt. Returns a cleanup function to remove
// the host temp file. No-op for sequential key type.
//
// When threads > 1, generates 2x the requested count to provide headroom for
// uneven sysbench event distribution across threads. Sysbench uses dynamic
// work-stealing — faster threads grab more events — so a thread can exhaust
// its UUID partition and overflow into the next thread's range, causing
// duplicate key errors or nil reads.
func PrepareUUIDs(containerName, keyType string, count, threads int) (func(), error) {
	if keyType == "sequential" {
		return func() {}, nil
	}

	generateCount := count
	if threads > 1 {
		generateCount = count * 2
	}

	hostPath, err := generateUUIDFile(keyType, generateCount)
	if err != nil {
		return nil, fmt.Errorf("generate UUID file: %w", err)
	}

	cleanup := func() {
		os.Remove(hostPath)
	}

	cmd := exec.Command("docker", "cp", hostPath, fmt.Sprintf("%s:/tmp/uuids.txt", containerName))
	if out, err := cmd.CombinedOutput(); err != nil {
		cleanup()
		return nil, fmt.Errorf("copy UUID file to container: %w (output: %s)", err, string(out))
	}

	return cleanup, nil
}

// generateUUIDFile generates count UUIDs of the given keyType and writes them
// as 32-char hex strings (one per line) to a temp file. Returns the host path.
func generateUUIDFile(keyType string, count int) (string, error) {
	tmpFile, err := os.CreateTemp("", "uuids-*.txt")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}

	var entropy *ulid.MonotonicEntropy
	var entropyMu sync.Mutex
	if keyType == "ulid_monotonic" {
		entropy = ulid.Monotonic(crand.Reader, 0)
	}

	for i := 0; i < count; i++ {
		var bytes [16]byte

		switch keyType {
		case "uuidv1":
			u, err := uuid.NewUUID()
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return "", fmt.Errorf("generate UUIDv1: %w", err)
			}
			bytes = u
		case "uuidv4":
			u := uuid.New()
			bytes = u
		case "uuidv7":
			u, err := uuid.NewV7()
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return "", fmt.Errorf("generate UUIDv7: %w", err)
			}
			bytes = u
		case "ulid":
			u := ulid.MustNew(ulid.Now(), crand.Reader)
			copy(bytes[:], u[:])
		case "ulid_monotonic":
			entropyMu.Lock()
			u := ulid.MustNew(ulid.Now(), entropy)
			entropyMu.Unlock()
			copy(bytes[:], u[:])
		default:
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return "", fmt.Errorf("unsupported key type for UUID pre-generation: %s", keyType)
		}

		fmt.Fprintln(tmpFile, hex.EncodeToString(bytes[:]))
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("close temp file: %w", err)
	}

	return tmpFile.Name(), nil
}
