package db

import (
"math/big"
"testing"
"time"

"github.com/antdaza/antdchain/antdc/rotatingking"
"github.com/cockroachdb/pebble"
"github.com/ethereum/go-ethereum/common"
)

func openTestPebble(t *testing.T) *pebble.DB {
t.Helper()
db, err := pebble.Open(t.TempDir(), &pebble.Options{})
if err != nil {
t.Fatalf("open pebble: %v", err)
}
t.Cleanup(func() { _ = db.Close() })
return db
}

func TestWriteBlockSyncRecordAndGetLast(t *testing.T) {
rdb := &PebbleRotatingKingDB{db: openTestPebble(t)}

records := []*rotatingking.BlockSyncRecord{
{BlockHeight: 100, BlockHash: common.HexToHash("0x1"), Timestamp: time.Unix(1000, 0)},
{BlockHeight: 101, BlockHash: common.HexToHash("0x2"), Timestamp: time.Unix(1001, 0)},
{BlockHeight: 150, BlockHash: common.HexToHash("0x3"), Timestamp: time.Unix(1002, 0)},
}

for _, r := range records {
if err := rdb.WriteBlockSyncRecord(r); err != nil {
t.Fatalf("write block sync record failed: %v", err)
}
}

got, err := rdb.GetLastBlockSyncRecord()
if err != nil {
t.Fatalf("GetLastBlockSyncRecord failed: %v", err)
}
if got == nil {
t.Fatal("expected non-nil last record")
}
if got.BlockHeight != 150 {
t.Fatalf("last record height = %d, want 150", got.BlockHeight)
}
}

func TestWriteAndGetRotationEventsInRange(t *testing.T) {
rdb := &PebbleRotatingKingDB{db: openTestPebble(t)}

events := []rotatingking.KingRotation{
{BlockHeight: 99, NewKing: common.HexToAddress("0x0000000000000000000000000000000000000001"), Timestamp: time.Unix(999, 0), Reward: big.NewInt(1)},
{BlockHeight: 100, NewKing: common.HexToAddress("0x0000000000000000000000000000000000000002"), Timestamp: time.Unix(1000, 0), Reward: big.NewInt(2)},
{BlockHeight: 101, NewKing: common.HexToAddress("0x0000000000000000000000000000000000000003"), Timestamp: time.Unix(1001, 0), Reward: big.NewInt(3)},
}

for i := range events {
if err := rdb.WriteRotationEvent(&events[i]); err != nil {
t.Fatalf("WriteRotationEvent failed: %v", err)
}
}

got, err := rdb.GetRotationEvents(100, 101)
if err != nil {
t.Fatalf("GetRotationEvents failed: %v", err)
}
if len(got) != 2 {
t.Fatalf("events in range = %d, want 2", len(got))
}
}
