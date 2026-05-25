package part_checksum

import "encoding/binary"

// SipHash128 implements the *ClickHouse* SipHash variant (zero key, non-reference
// 128-bit mode), matching src/Common/SipHash.h. Note this is NOT standard
// SipHash-2-4-128: init has no `v1 ^= 0xee`, finalize uses `v2 ^= 0xff`, and the
// 128-bit output is just `(v0^v1, v2^v3)` without the second finalization round.
type SipHash128 struct {
	v0, v1, v2, v3 uint64
	buf            [8]byte
	cnt            int
	totalLen       uint64
}

func NewSipHash128() *SipHash128 {
	return &SipHash128{
		v0: 0x736f6d6570736575,
		v1: 0x646f72616e646f6d,
		v2: 0x6c7967656e657261,
		v3: 0x7465646279746573,
	}
}

func rotl(x uint64, b uint) uint64 {
	return (x << b) | (x >> (64 - b))
}

// sipRound matches the SIPROUND macro in src/Common/SipHash.h.
func (s *SipHash128) sipRound() {
	s.v0 += s.v1
	s.v1 = rotl(s.v1, 13)
	s.v1 ^= s.v0
	s.v0 = rotl(s.v0, 32)

	s.v2 += s.v3
	s.v3 = rotl(s.v3, 16)
	s.v3 ^= s.v2

	s.v0 += s.v3
	s.v3 = rotl(s.v3, 21)
	s.v3 ^= s.v0

	s.v2 += s.v1
	s.v1 = rotl(s.v1, 17)
	s.v1 ^= s.v2
	s.v2 = rotl(s.v2, 32)
}

func (s *SipHash128) Write(data []byte) {
	s.totalLen += uint64(len(data))
	idx := 0

	if s.cnt > 0 {
		for idx < len(data) && s.cnt < 8 {
			s.buf[s.cnt] = data[idx]
			s.cnt++
			idx++
		}
		if s.cnt == 8 {
			m := binary.LittleEndian.Uint64(s.buf[:])
			s.v3 ^= m
			s.sipRound()
			s.sipRound()
			s.v0 ^= m
			s.cnt = 0
		}
	}

	for idx+8 <= len(data) {
		m := binary.LittleEndian.Uint64(data[idx:])
		s.v3 ^= m
		s.sipRound()
		s.sipRound()
		s.v0 ^= m
		idx += 8
	}

	for idx < len(data) {
		s.buf[s.cnt] = data[idx]
		s.cnt++
		idx++
	}
}

// Sum128 finalizes the hash and returns (low64=v0^v1, high64=v2^v3). The hex
// representation as printed by ClickHouse's getHexUIntLowercase is `high64`
// followed by `low64`, both lowercase, padded to 16 hex chars each.
func (s *SipHash128) Sum128() (lo, hi uint64) {
	// current_word with cnt%256 in the high byte (low 7 bytes contain the
	// tail bytes buffered in s.buf).
	var current [8]byte
	copy(current[:], s.buf[:s.cnt])
	current[7] = byte(s.totalLen & 0xff)
	word := binary.LittleEndian.Uint64(current[:])

	v0, v1, v2, v3 := s.v0, s.v1, s.v2, s.v3

	v3 ^= word
	for r := 0; r < 2; r++ {
		v0 += v1
		v1 = rotl(v1, 13)
		v1 ^= v0
		v0 = rotl(v0, 32)
		v2 += v3
		v3 = rotl(v3, 16)
		v3 ^= v2
		v0 += v3
		v3 = rotl(v3, 21)
		v3 ^= v0
		v2 += v1
		v1 = rotl(v1, 17)
		v1 ^= v2
		v2 = rotl(v2, 32)
	}
	v0 ^= word
	v2 ^= 0xff
	for r := 0; r < 4; r++ {
		v0 += v1
		v1 = rotl(v1, 13)
		v1 ^= v0
		v0 = rotl(v0, 32)
		v2 += v3
		v3 = rotl(v3, 16)
		v3 ^= v2
		v0 += v3
		v3 = rotl(v3, 21)
		v3 ^= v0
		v2 += v1
		v1 = rotl(v1, 17)
		v1 ^= v2
		v2 = rotl(v2, 32)
	}
	return v0 ^ v1, v2 ^ v3
}
