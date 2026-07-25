package backup

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/stretchr/testify/require"
)

func newTestRebalanceInfo() *rebalanceDisksInfo {
	return &rebalanceDisksInfo{
		liveTypes:     map[string]string{"default": "local", "hdd1": "local", "hdd2": "local", "s3disk": "s3"},
		objectDisks:   map[string]struct{}{"s3disk": {}},
		skipDisks:     map[string]struct{}{},
		effectiveFree: map[string]uint64{"default": 1000, "hdd1": 1000, "hdd2": 1000},
	}
}

func newTestTableMetadata(parts map[string][]metadata.Part) *metadata.TableMetadata {
	return &metadata.TableMetadata{
		Database: "db",
		Table:    "t",
		Query:    "CREATE TABLE db.t (id UInt64) ENGINE=MergeTree() ORDER BY id SETTINGS storage_policy = 'jbod'",
		Parts:    parts,
		Size:     map[string]int64{},
	}
}

func noSizeResolver(t *testing.T) func(string, string) uint64 {
	return func(physicalSrcDisk, partName string) uint64 {
		t.Fatalf("resolvePartSize must not be called for %s/%s", physicalSrcDisk, partName)
		return 0
	}
}

func TestComputeTableRebalancePlan(t *testing.T) {
	jbodDisks := map[string]struct{}{"hdd1": {}, "hdd2": {}}
	localTypes := map[string]string{"default": "local", "hdd1": "local", "hdd2": "local", "dead_disk": "local"}

	t.Run("rule1 live part on the same disk", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "hdd1", Path: "/hdd1_data/data/db/t/all_1_1_0/"}}
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("rule2 live part on other disk produces hardlink move", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "hdd2", Path: "/hdd2_data/data/db/t/all_1_1_0/"}}
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Equal(t, []partMove{{
			PartName: "all_1_1_0", SrcDisk: "hdd1", PhysicalSrcDisk: "hdd1", DstDisk: "hdd2",
			HardlinkFromLive: true, LivePartPath: "/hdd2_data/data/db/t/all_1_1_0/", Size: 10, Reason: "align_to_system_parts",
		}}, moves)
	})

	t.Run("rule1 respects RebalancedDisk redirect from previous download", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {{Name: "all_1_1_0", RebalancedDisk: "hdd2", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "hdd2", Path: "/hdd2_data/data/db/t/all_1_1_0/"}}
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("rule2 physical source follows RebalancedDisk redirect", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {{Name: "all_1_1_0", RebalancedDisk: "hdd2", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "hdd1", Path: "/hdd1_data/data/db/t/all_1_1_0/"}}
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Len(t, moves, 1)
		require.Equal(t, "dead_disk", moves[0].SrcDisk)
		require.Equal(t, "hdd2", moves[0].PhysicalSrcDisk)
		require.Equal(t, "hdd1", moves[0].DstDisk)
		require.True(t, moves[0].HardlinkFromLive)
	})

	t.Run("required parts skipped", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {{Name: "all_1_1_0", Required: true}}})
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("backup disk with object storage blobs skipped", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"s3disk": {{Name: "all_1_1_0", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "hdd1", Path: "/hdd1_data/data/db/t/all_1_1_0/"}}
		backupObjectDisks := map[string]struct{}{"s3disk": {}}
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, backupObjectDisks, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("source disk matched by skip_disks skipped", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "hdd2", Path: "/hdd2_data/data/db/t/all_1_1_0/"}}
		info := newTestRebalanceInfo()
		info.skipDisks["hdd1"] = struct{}{}
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, nil, info, noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("live part on object disk with valid source disk stays", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "s3disk", Path: "/s3disk/store/xxx/all_1_1_0/"}}
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("live part on non-policy disk with invalid source falls back to rule3", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {{Name: "all_1_1_0", Size: 10}}})
		liveParts := map[string]livePart{"all_1_1_0": {Name: "all_1_1_0", DiskName: "default", Path: "/var/lib/clickhouse/data/db/t/all_1_1_0/"}}
		info := newTestRebalanceInfo()
		info.effectiveFree["hdd1"] = 500
		info.effectiveFree["hdd2"] = 100
		moves, err := computeTableRebalancePlan(tm, localTypes, liveParts, "jbod", jbodDisks, nil, info, noSizeResolver(t))
		require.NoError(t, err)
		require.Len(t, moves, 1)
		require.False(t, moves[0].HardlinkFromLive)
		require.Equal(t, "hdd1", moves[0].DstDisk)
		require.Equal(t, "invalid_backup_disk", moves[0].Reason)
	})

	t.Run("rule3 valid disk with equal free space stays", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 10}}})
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("rule3 valid disk moves to strictly roomier disk", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 10}}})
		info := newTestRebalanceInfo()
		info.effectiveFree["hdd1"] = 100
		info.effectiveFree["hdd2"] = 500
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, info, noSizeResolver(t))
		require.NoError(t, err)
		require.Len(t, moves, 1)
		require.False(t, moves[0].HardlinkFromLive)
		require.Equal(t, "hdd2", moves[0].DstDisk)
		require.Equal(t, "least_used_disk", moves[0].Reason)
		require.Equal(t, uint64(490), info.effectiveFree["hdd2"])
	})

	t.Run("rule3 valid disk stays when the target is not roomier after landing", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 10}}})
		info := newTestRebalanceInfo()
		info.effectiveFree["hdd1"] = 100
		info.effectiveFree["hdd2"] = 105
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, info, noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("rule3 valid disk stays without error when no disk fits", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"hdd1": {{Name: "all_1_1_0", Size: 5000}}})
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Empty(t, moves)
	})

	t.Run("rule3 disk not in policy is invalid", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"default": {{Name: "all_1_1_0", Size: 10}}})
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.NoError(t, err)
		require.Len(t, moves, 1)
		require.Equal(t, "invalid_backup_disk", moves[0].Reason)
	})

	t.Run("rule3 sequential allocation spills to the next disk", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {
			{Name: "all_1_1_0", Size: 400},
			{Name: "all_2_2_0", Size: 400},
		}})
		info := newTestRebalanceInfo()
		info.effectiveFree["hdd1"] = 500
		info.effectiveFree["hdd2"] = 450
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, info, noSizeResolver(t))
		require.NoError(t, err)
		require.Len(t, moves, 2)
		require.Equal(t, "hdd1", moves[0].DstDisk)
		require.Equal(t, "hdd2", moves[1].DstDisk)
		require.Equal(t, uint64(100), info.effectiveFree["hdd1"])
		require.Equal(t, uint64(50), info.effectiveFree["hdd2"])
	})

	t.Run("rule3 no disk with enough free space returns error", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {{Name: "all_1_1_0", Size: 5000}}})
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.Error(t, err)
		require.Contains(t, err.Error(), "all_1_1_0")
		require.Nil(t, moves)
	})

	t.Run("rule3 destination disk type must match backup disk type", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {{Name: "all_1_1_0", Size: 10}}})
		diskTypes := map[string]string{"dead_disk": "s3"}
		_, err := computeTableRebalancePlan(tm, diskTypes, nil, "jbod", jbodDisks, nil, newTestRebalanceInfo(), noSizeResolver(t))
		require.Error(t, err)
		require.Contains(t, err.Error(), "type `s3`")
	})

	t.Run("rule3 zero size resolved via callback", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{"dead_disk": {{Name: "all_1_1_0"}}})
		resolved := false
		resolver := func(physicalSrcDisk, partName string) uint64 {
			require.Equal(t, "dead_disk", physicalSrcDisk)
			require.Equal(t, "all_1_1_0", partName)
			resolved = true
			return 42
		}
		moves, err := computeTableRebalancePlan(tm, localTypes, nil, "jbod", jbodDisks, nil, newTestRebalanceInfo(), resolver)
		require.NoError(t, err)
		require.True(t, resolved)
		require.Len(t, moves, 1)
		require.Equal(t, uint64(42), moves[0].Size)
	})
}

func TestChooseRebalanceDstDisk(t *testing.T) {
	policyDisks := map[string]struct{}{"hdd1": {}, "hdd2": {}, "s3disk": {}}

	t.Run("max effective free wins", func(t *testing.T) {
		info := newTestRebalanceInfo()
		info.effectiveFree["hdd1"] = 100
		info.effectiveFree["hdd2"] = 200
		disk, err := chooseRebalanceDstDisk("local", 50, policyDisks, info)
		require.NoError(t, err)
		require.Equal(t, "hdd2", disk)
	})

	t.Run("tie broken by disk name", func(t *testing.T) {
		info := newTestRebalanceInfo()
		info.effectiveFree["hdd1"] = 200
		info.effectiveFree["hdd2"] = 200
		disk, err := chooseRebalanceDstDisk("local", 50, policyDisks, info)
		require.NoError(t, err)
		require.Equal(t, "hdd1", disk)
	})

	t.Run("object and skipped disks excluded", func(t *testing.T) {
		info := newTestRebalanceInfo()
		info.effectiveFree["s3disk"] = 100000
		info.skipDisks["hdd2"] = struct{}{}
		info.effectiveFree["hdd2"] = 100000
		disk, err := chooseRebalanceDstDisk("local", 50, policyDisks, info)
		require.NoError(t, err)
		require.Equal(t, "hdd1", disk)
	})

	t.Run("no fit returns error", func(t *testing.T) {
		info := newTestRebalanceInfo()
		_, err := chooseRebalanceDstDisk("local", 100000, policyDisks, info)
		require.Error(t, err)
	})
}

func TestApplyMoveToTableMetadata(t *testing.T) {
	t.Run("part entry moved sorted and sizes updated", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{
			"hdd1": {{Name: "all_1_1_0", Size: 10}, {Name: "all_3_3_0", Size: 30}},
			"hdd2": {{Name: "all_2_2_0", Size: 20}},
		})
		tm.Size = map[string]int64{"hdd1": 40, "hdd2": 20}
		applyMoveToTableMetadata(tm, partMove{PartName: "all_1_1_0", SrcDisk: "hdd1", PhysicalSrcDisk: "hdd1", DstDisk: "hdd2", Size: 10})
		require.Equal(t, []metadata.Part{{Name: "all_3_3_0", Size: 30}}, tm.Parts["hdd1"])
		require.Equal(t, []metadata.Part{{Name: "all_1_1_0", Size: 10}, {Name: "all_2_2_0", Size: 20}}, tm.Parts["hdd2"])
		require.Equal(t, int64(30), tm.Size["hdd1"])
		require.Equal(t, int64(30), tm.Size["hdd2"])
	})

	t.Run("empty source keys removed and RebalancedDisk cleared", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{
			"dead_disk": {{Name: "all_1_1_0", RebalancedDisk: "hdd2", Size: 10}},
		})
		tm.Size = map[string]int64{"dead_disk": 10}
		applyMoveToTableMetadata(tm, partMove{PartName: "all_1_1_0", SrcDisk: "dead_disk", PhysicalSrcDisk: "hdd2", DstDisk: "hdd1", Size: 10})
		require.NotContains(t, tm.Parts, "dead_disk")
		require.NotContains(t, tm.Size, "dead_disk")
		require.Equal(t, []metadata.Part{{Name: "all_1_1_0", Size: 10}}, tm.Parts["hdd1"])
		require.Equal(t, int64(10), tm.Size["hdd1"])
	})

	t.Run("archive files renamed for downloaded backups", func(t *testing.T) {
		tm := newTestTableMetadata(map[string][]metadata.Part{
			"hdd1": {{Name: "all_1_1_0", Size: 10}, {Name: "all_2_2_0", Size: 20}},
		})
		tm.Files = map[string][]string{"hdd1": {"hdd1_all_1_1_0.tar.gz", "hdd1_all_2_2_0.tar.gz"}}
		applyMoveToTableMetadata(tm, partMove{PartName: "all_1_1_0", SrcDisk: "hdd1", PhysicalSrcDisk: "hdd1", DstDisk: "hdd2", Size: 10})
		require.Equal(t, []string{"hdd1_all_2_2_0.tar.gz"}, tm.Files["hdd1"])
		require.Equal(t, []string{"hdd2_all_1_1_0.tar.gz"}, tm.Files["hdd2"])
	})
}
