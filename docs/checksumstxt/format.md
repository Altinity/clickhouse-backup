# `checksums.txt` — Formal Format Specification

`checksums.txt` is a per-part metadata file written by `MergeTree` data parts. Reference implementation: `src/Storages/MergeTree/MergeTreeDataPartChecksum.{h,cpp}`.

## 1. Top-level structure

```
checksums.txt := header LF body
header        := "checksums format version: " UINT_DEC
LF            := 0x0A
```

* `UINT_DEC` is an unsigned integer in plain decimal ASCII (no leading zeros required, no sign).
* The version determines `body` layout. Known versions:

| Version | Body encoding                                                          | Used for `checksums.txt`?                                                                                       |
| ------: | ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| 1       | (legacy, unsupported by current code; reader returns "format too old") | no longer written                                                                                               |
| 2       | Text                                                                   | yes (legacy)                                                                                                    |
| 3       | Binary, uncompressed                                                   | yes (legacy)                                                                                                    |
| 4       | Binary, framed in one ClickHouse compressed-block                      | **default written today**                                                                                       |
| 5       | "Minimalistic" (totals only)                                           | **only used for `MinimalisticDataPartChecksums`, e.g. ZooKeeper payload — not for the on-disk `checksums.txt`** |

A robust parser must support v2, v3, v4 for the on-disk file and v5 only when reading the minimalistic blob.

After the body, there must be EOF (the writer never appends anything past the body).

## 2. Common primitive encodings

These are ClickHouse's standard binary primitives (used inside the body for v3/v4):

| Name                   | Encoding                                                                                                                                              |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `VarUInt(x)`           | LEB128 / Variable Byte. Repeated bytes `0x80 \| (x & 0x7F)` while `x > 0x7F`, then a final byte `x & 0x7F`. Up to 10 bytes for `UInt64`.              |
| `BinaryLE(uint8/bool)` | exactly 1 byte; for `bool`, `0` = false, anything else = true (writer emits `0` or `1`).                                                              |
| `BinaryLE(UInt32)`     | 4 bytes, little-endian.                                                                                                                               |
| `BinaryLE(UInt64)`     | 8 bytes, little-endian.                                                                                                                               |
| `BinaryLE(uint128)`    | The `CityHash_v1_0_2::uint128` struct is `{ UInt64 low64; UInt64 high64; }`. Serialized as `BinaryLE(low64)` then `BinaryLE(high64)`, total 16 bytes. |
| `StringBinary(s)`      | `VarUInt(len(s))` followed by `len(s)` raw bytes. No NUL terminator. UTF-8 in practice (file names).                                                  |

## 3. Body layouts

### 3.1. Version 2 — text body

Grammar (whitespace shown explicitly; `\n` = LF; `\t` = HT):

```
body_v2     := count " files:\n" record{count}
record      := name "\n"
               "\tsize: " UINT_DEC "\n"
               "\thash: " UINT_DEC " " UINT_DEC "\n"
               "\tcompressed: " BOOL_DEC
               [ "\n\tuncompressed size: " UINT_DEC
                 "\n\tuncompressed hash: " UINT_DEC " " UINT_DEC ]
               "\n"
```

* `count` is decimal ASCII.
* `name` is read as a raw line up to (but not including) the next `\n`. The reference uses `readString`, which reads bytes until `\n` is encountered; backslash-escaping is *not* applied here. (File names in part directories don't normally contain `\n`.)
* `BOOL_DEC` is `0` or `1`. The optional `uncompressed …` block is present iff `compressed = 1`.
* The two `UINT_DEC` after `hash:` / `uncompressed hash:` are the `low64` then `high64` of the `uint128`, printed in **decimal**.

### 3.2. Version 3 — binary body, no compression

```
body_v3 := VarUInt(count) record{count}
record  := StringBinary(name)
           VarUInt(file_size)
           BinaryLE(uint128 file_hash)         // 16 bytes
           BinaryLE(bool    is_compressed)     // 1 byte
           if is_compressed:
               VarUInt(uncompressed_size)
               BinaryLE(uint128 uncompressed_hash)  // 16 bytes
```

The map ordering on disk is whatever the writer produced (the in-memory container is an ordered `std::map<String, Checksum>`, so v4-written files are in lexicographic order of `name`, but a parser should not rely on order for correctness).

### 3.3. Version 4 — binary body, wrapped in a compressed-block stream

`body_v4` is a sequence of one or more **ClickHouse compressed blocks**. Concatenating the *uncompressed payloads* of these blocks yields exactly a `body_v3` byte stream.

In practice the writer emits a single block (buffer 64 KiB, default codec LZ4), but a parser MUST handle multi-block streams (loop until the underlying buffer is exhausted; the inner `body_v3` parser will consume exactly the right amount).

#### 3.3.1. Compressed-block frame

Each block on the wire:

```
block        := checksum128 method size_compressed size_uncompressed payload
checksum128  := 16 bytes        // CityHash128 of: method || size_compressed || size_uncompressed || payload
method       := 1 byte          // codec id (see below)
size_compressed   := 4 bytes LE // INCLUDES the 9-byte header (method+the two sizes), EXCLUDES the 16-byte checksum
size_uncompressed := 4 bytes LE
payload      := size_compressed - 9 bytes of codec-specific data
```

Constraint: `size_compressed <= 0x40000000` (1 GiB); reject otherwise.

The CityHash128 is `CityHash_v1_0_2::CityHash128` over the 9 header bytes followed by `size_compressed - 9` payload bytes (i.e. the bytes immediately following the checksum, totalling `size_compressed` bytes). A strict parser should verify it; a lenient parser may skip verification.

#### 3.3.2. Codec method bytes

Only the codecs ClickHouse may use to compress small metadata are realistically encountered, but a generic parser should be prepared:

| `method` | Codec                          | Payload semantics                                                                                                                                                                              |
| -------: | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `0x02`   | NONE                           | raw bytes; uncompressed payload = compressed payload                                                                                                                                           |
| `0x82`   | LZ4 / LZ4HC (same wire format) | LZ4 block of `size_compressed - 9` bytes that decompresses to exactly `size_uncompressed` bytes                                                                                                |
| `0x90`   | ZSTD                           | ZSTD frame, decompresses to `size_uncompressed` bytes                                                                                                                                          |
| `0x91`   | Multiple                       | wrapper; first payload byte is the number of nested codecs followed by their method bytes, then the inner compressed stream — rarely used for `checksums.txt`. See `CompressionCodecMultiple`. |

For `checksums.txt` written by current ClickHouse, the default codec is the server's `default` codec (typically LZ4 → `0x82`). All multi-byte integers are little-endian.

After fully decompressing all blocks and concatenating, parse the result as `body_v3` (§3.2).

### 3.4. Version 5 — minimalistic (NOT on-disk checksums.txt)

For completeness; a parser of `checksums.txt` may reject this version, since current code never writes v5 to disk. It IS the format used for `MinimalisticDataPartChecksums` in ZooKeeper.

```
body_v5 := VarUInt(num_compressed_files)
           VarUInt(num_uncompressed_files)
           BinaryLE(uint128 hash_of_all_files)
           BinaryLE(uint128 hash_of_uncompressed_files)
           BinaryLE(uint128 uncompressed_hash_of_compressed_files)
```

The header line for a v5 blob is `"checksums format version: 5\n"`.

## 4. Logical model produced by the parser

After parsing v2/v3/v4, the parser yields a map `name → Checksum`:

```
struct Checksum {
    UInt64  file_size;
    uint128 file_hash;          // CityHash128 of the file's bytes on disk (compressed bytes if file is compressed)
    bool    is_compressed;
    UInt64  uncompressed_size;  // valid iff is_compressed
    uint128 uncompressed_hash;  // CityHash128 of decompressed bytes, valid iff is_compressed
}
```

Semantics:
* `file_hash` / `file_size` describe bytes as stored on disk.
* For files that ClickHouse stores using its compressed-block format (most `*.bin` column data), `is_compressed = true` and `uncompressed_*` describe the concatenation of the decompressed block payloads (i.e. logical column-data bytes).
* The map keys are paths *relative to the part directory* (e.g. `columns.txt`, `primary.idx`, `id.bin`, `id.cmrk2`, …). Subdirectory entries (projections) appear with `/`-separated paths.

## 5. Validation rules a strict parser should enforce

1. The first line MUST start with the literal `checksums format version: ` (note the trailing space) and end with `\n`.
2. Version MUST be one of 2, 3, 4 for the on-disk `checksums.txt` (5 only for the minimalistic blob).
3. For v2, every literal token (`" files:\n"`, `"\n\tsize: "`, `"\n\thash: "`, `"\n\tcompressed: "`, `"\n\tuncompressed size: "`, `"\n\tuncompressed hash: "`, the trailing `"\n"`, and the inter-field single-space separator between the two halves of `uint128`) MUST match exactly.
4. For v3/v4, `count` MUST be consumable; each record MUST be fully consumed.
5. For v4 frames: `size_compressed >= 9`, `size_compressed <= 1 GiB`, and (recommended) checksum verification.
6. After the last record (and after all compressed blocks for v4), there MUST be no trailing bytes — `assertEOF` is called by the reference implementation in `deserializeFrom`.
7. `name` MUST be unique within `files` (the writer uses a `std::map`, so duplicates indicate corruption).

## 6. Worked v3 byte-level example

A `checksums.txt` with one entry `columns.txt` of size 123 and `is_compressed = false` is exactly:

```
"checksums format version: 3\n"      // header line, ASCII
01                                    // VarUInt(count=1)
0B                                    // VarUInt(11)  -- length of "columns.txt"
63 6F 6C 75 6D 6E 73 2E 74 78 74      // "columns.txt"
7B                                    // VarUInt(123)
<16 bytes uint128 file_hash, low64 LE then high64 LE>
00                                    // is_compressed = false
                                      // (no uncompressed_* fields)
EOF
```

## 7. Pointers into the source

* `MergeTreeDataPartChecksums::read` / `write` — top-level dispatch + v3/v4 body. (`src/Storages/MergeTree/MergeTreeDataPartChecksum.cpp:115-240`)
* `MergeTreeDataPartChecksums::readV2` — text body. (same file, lines 145-181)
* `MinimalisticDataPartChecksums::serialize/deserialize` — v5 body. (same file, lines 334-391)
* `CompressedReadBuffer` / `CompressionInfo.h` — compressed-block frame used by v4. (`src/Compression/CompressionInfo.h`)
* `VarInt.h` — `VarUInt` encoding. (`src/IO/VarInt.h`)
* `CityHash_v1_0_2::uint128` — hash type, `{ low64, high64 }`.

## 8. Implementation Summary

`checksumstxt/`:

- **`checksumstxt.go`** — `Parse(io.Reader) → *File` for versions 2/3/4, `ParseMinimalistic(io.Reader) → *Minimalistic` for version 5. Returns a typed `Hash128 = {Low, High uint64}` and a `Checksum` struct matching the C++ `MergeTreeDataPartChecksum` shape.
- **`checksumstxt_test.go`** — round-trips for v2 (text), v3 (raw binary), v4 with **LZ4 / NONE / ZSTD** codecs and a **multi-block** stream, the v5 minimalistic blob, and rejection cases (trailing bytes, v1, v5-via-Parse, unknown).

### What's reused vs. new

- **Reused** (transitive deps via `ch-go`): `chproto.Reader` for `UVarInt` / `Str` / `UInt128` (already `{Low: LE0..7, High: LE8..15}`) / `Bool`. The whole v4 framing — 16-byte CityHash128 + 9-byte header + LZ4/ZSTD/NONE — is handled by `compress.Reader`, surfaced through one call: `pr.EnableCompression()` on a `chproto.Reader` switches the v3 record loop to read decompressed bytes, so v3 and v4 share the same code path.
- **Reused** (already in this repo): `lib/cityhash102.CityHash128` is available if you ever want to validate v4 frames yourself (not needed — `compress.Reader` verifies internally).
- **New**: header-line dispatcher, v2 line-oriented parser, v3 record loop, v5 (5 fields, trivial), and EOF assertions for each path.

### Path notes & limitations

- `Multiple` codec (`0x91`) is **not** handled by `ch-go/compress.Reader`. Per the spec it isn't used for `checksums.txt`, so the parser surfaces a "compression 0x91 not implemented" error if encountered — matching the spec's "rarely used" note.
- v1 is rejected with "format too old", matching the C++ reference.
- `Parse` returns an error for v5 (and `ParseMinimalistic` rejects non-5) so you can't accidentally cross the wires.

