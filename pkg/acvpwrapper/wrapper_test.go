package acvpwrapper

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetConfig(t *testing.T) {
	in := bytes.NewBuffer(encodeRequest("getConfig"))
	var out bytes.Buffer
	err := processingLoop(in, &out)
	require.NoError(t, err)

	resp, err := decodeResponse(&out)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	require.Contains(t, string(resp[0]), `"algorithm": "SHA2-256"`)
	require.Contains(t, string(resp[0]), `"algorithm": "ACVP-AES-GCM"`)
	require.NotContains(t, string(resp[0]), `"algorithm": "ML-KEM"`)
	require.NotContains(t, string(resp[0]), `"algorithm": "ML-DSA"`)
}

func TestSHA256AFT(t *testing.T) {
	msg := []byte("abc")
	in := bytes.NewBuffer(encodeRequest("SHA2-256", msg))
	var out bytes.Buffer

	err := processingLoop(in, &out)
	require.NoError(t, err)

	resp, err := decodeResponse(&out)
	require.NoError(t, err)
	require.Len(t, resp, 1)

	sum := sha256.Sum256(msg)
	require.Equal(t, hex.EncodeToString(sum[:]), hex.EncodeToString(resp[0]))
}

func decodeResponse(r io.Reader) ([][]byte, error) {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	lengths := make([]uint32, count)
	for i := range lengths {
		if err := binary.Read(r, binary.LittleEndian, &lengths[i]); err != nil {
			return nil, err
		}
	}
	args := make([][]byte, count)
	for i, n := range lengths {
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = buf
	}
	return args, nil
}
