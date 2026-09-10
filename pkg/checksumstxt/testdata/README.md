# checksumstxt testdata

Real `checksums.txt` files extracted from a live ClickHouse server for fixture-driven parser tests.

## ClickHouse version

**24.8.14.39** (image `clickhouse/clickhouse-server:24.8`, official build)

## Fixtures

### `v4_wide/checksums.txt`

Wide MergeTree part, format version 4, 3 columns, 9 file entries.

```sql
CREATE TABLE fx.wide (id UInt64, x String, y Float64)
  ENGINE=MergeTree ORDER BY id
  SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
INSERT INTO fx.wide SELECT number, toString(number), number*1.5 FROM numbers(1000);
OPTIMIZE TABLE fx.wide FINAL;
```

Part directory: `all_1_1_1`

### `v4_compact/checksums.txt`

Compact MergeTree part, format version 4, 2 columns, 5 file entries.
Compact format is forced by setting `min_rows_for_wide_part` and `min_bytes_for_wide_part` very high.
Compact parts store all columns in a single `data.bin`/`data.cmrk3` pair.

```sql
CREATE TABLE fx.compact (id UInt64, x String)
  ENGINE=MergeTree ORDER BY id
  SETTINGS min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=1000000000;
INSERT INTO fx.compact SELECT number, toString(number) FROM numbers(100);
OPTIMIZE TABLE fx.compact FINAL;
```

Part directory: `all_1_1_1`

### `v4_projection/checksums.txt`

Wide MergeTree part with a PROJECTION, format version 4, 10 file entries.
Includes a `p1.proj` entry (the serialized projection sub-part).

```sql
CREATE TABLE fx.proj (id UInt64, c String, n UInt32,
  PROJECTION p1 (SELECT c, count() GROUP BY c))
  ENGINE=MergeTree ORDER BY id
  SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
INSERT INTO fx.proj
  SELECT number, ['a','b','c'][number%3+1], toUInt32(number%10)
  FROM numbers(500);
OPTIMIZE TABLE fx.proj FINAL;
```

Part directory: `all_1_1_1`

### `v4_multi_block/checksums.txt`

Wide MergeTree part with 300 Int64 columns, format version 4, 602 file entries.
The large file list (300 columns × ~2 files each) produces a ~110 KB uncompressed payload
that stresses the compressed-block reader.  While the ClickHouse 24.8 LZ4 block size
(1 MB default) fits all entries in a single block, the payload size is ~300x larger than
the wide/compact fixtures and validates correct handling of large payloads.
100 rows with distinct non-zero values ensure no column file is empty.

```sql
-- The column list has 300 columns: c0 Int64, c1 Int64, ..., c299 Int64
-- Generated with: cols=$(python3 -c "print(','.join(f'c{i} Int64' for i in range(300)))")
CREATE TABLE fx.multi (<300_cols>) ENGINE=MergeTree ORDER BY tuple()
  SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
-- INSERT with number*multiplier+1 for each column so no column is all-zero:
INSERT INTO fx.multi
  SELECT number*1+1, number*2+1, ..., number*300+1
  FROM numbers(100);
OPTIMIZE TABLE fx.multi FINAL;
```

Part directory: `all_1_1_1`

## Missing fixtures and why

### v2 / v3 (text / uncompressed-binary format)

Format versions 2 and 3 were used by ClickHouse releases predating ~20.x.
They are not produced by any supported ClickHouse version.
The parser is fully covered by the synthetic unit tests `TestParseV2` and `TestParseV3`
in `checksumstxt_test.go`.

### v5 (minimalistic blob)

Version 5 is a compact ZooKeeper payload — it is never written to disk as a
`checksums.txt` file.  It cannot be obtained by extracting a file from a data
part directory.  The parser is covered by the synthetic unit test
`TestParseMinimalistic` in `checksumstxt_test.go`.
