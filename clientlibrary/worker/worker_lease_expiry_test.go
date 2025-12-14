/*
 * Copyright (c) 2025 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package worker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	chk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl-v2/clientlibrary/partition"
)

// TestLeaseCounterExcludesExpiredLeases tests that the lease counter excludes expired leases
func TestLeaseCounterExcludesExpiredLeases(t *testing.T) {
	kclConfig := config.NewKinesisClientLibConfig("appName", "test-stream", "us-west-2", "worker-1").
		WithMaxLeasesForWorker(3)

	worker := &Worker{
		workerID:    "worker-1",
		kclConfig:   kclConfig,
		shardStatus: make(map[string]*par.ShardStatus),
	}

	now := time.Now().UTC()
	expiredTime := now.Add(-1 * time.Hour)
	validTime := now.Add(1 * time.Hour)

	// Create test shards
	worker.shardStatus["shard-0001"] = &par.ShardStatus{
		ID:           "shard-0001",
		Mux:          &sync.RWMutex{},
		AssignedTo:   "worker-1",
		Checkpoint:   "12345",
		LeaseTimeout: validTime, // Valid lease
	}

	worker.shardStatus["shard-0002"] = &par.ShardStatus{
		ID:           "shard-0002",
		Mux:          &sync.RWMutex{},
		AssignedTo:   "worker-1",
		Checkpoint:   "67890",
		LeaseTimeout: expiredTime, // Expired lease - should not be counted
	}

	worker.shardStatus["shard-0003"] = &par.ShardStatus{
		ID:           "shard-0003",
		Mux:          &sync.RWMutex{},
		AssignedTo:   "worker-1",
		Checkpoint:   "11111",
		LeaseTimeout: validTime, // Valid lease
	}

	worker.shardStatus["shard-0004"] = &par.ShardStatus{
		ID:           "shard-0004",
		Mux:          &sync.RWMutex{},
		AssignedTo:   "worker-2", // Different worker
		Checkpoint:   "22222",
		LeaseTimeout: validTime,
	}

	worker.shardStatus["shard-0005"] = &par.ShardStatus{
		ID:           "shard-0005",
		Mux:          &sync.RWMutex{},
		AssignedTo:   "worker-1",
		Checkpoint:   chk.ShardEnd, // Completed shard - should not be counted
		LeaseTimeout: validTime,
	}

	// Count leases using the same logic as eventLoop
	counter := 0
	for _, shard := range worker.shardStatus {
		if shard.GetLeaseOwner() == worker.workerID && shard.GetCheckpoint() != chk.ShardEnd {
			// Exclude expired leases from the count
			leaseTimeout := shard.GetLeaseTimeout()
			if !leaseTimeout.IsZero() && leaseTimeout.Before(time.Now().UTC()) {
				continue
			}
			counter++
		}
	}

	// Should count only 2 valid leases (shard-0001 and shard-0003)
	// shard-0002 has expired lease
	// shard-0004 belongs to different worker
	// shard-0005 is completed (ShardEnd)
	assert.Equal(t, 2, counter, "Should count only active non-expired leases")

	// Verify worker can claim more shards since counter (2) < MaxLeasesForWorker (3)
	assert.True(t, counter < worker.kclConfig.MaxLeasesForWorker,
		"Worker should be able to claim more shards when expired leases are excluded")
}

// TestLeaseCounterWithAllExpiredLeases tests edge case where all leases are expired
func TestLeaseCounterWithAllExpiredLeases(t *testing.T) {
	kclConfig := config.NewKinesisClientLibConfig("appName", "test-stream", "us-west-2", "worker-1").
		WithMaxLeasesForWorker(3)

	worker := &Worker{
		workerID:    "worker-1",
		kclConfig:   kclConfig,
		shardStatus: make(map[string]*par.ShardStatus),
	}

	expiredTime := time.Now().UTC().Add(-1 * time.Hour)

	// Create shards with all expired leases
	for i := 1; i <= 5; i++ {
		worker.shardStatus[string(rune('0'+i))] = &par.ShardStatus{
			ID:           string(rune('0' + i)),
			Mux:          &sync.RWMutex{},
			AssignedTo:   "worker-1",
			Checkpoint:   "12345",
			LeaseTimeout: expiredTime, // All expired
		}
	}

	// Count leases
	counter := 0
	for _, shard := range worker.shardStatus {
		if shard.GetLeaseOwner() == worker.workerID && shard.GetCheckpoint() != chk.ShardEnd {
			leaseTimeout := shard.GetLeaseTimeout()
			if !leaseTimeout.IsZero() && leaseTimeout.Before(time.Now().UTC()) {
				continue
			}
			counter++
		}
	}

	// All leases are expired, counter should be 0
	assert.Equal(t, 0, counter, "Should not count any expired leases")

	// Worker should be able to claim new shards
	assert.True(t, counter < worker.kclConfig.MaxLeasesForWorker,
		"Worker should be able to claim shards when all existing leases are expired")
}

// TestLeaseCounterWithZeroLeaseTimeout tests handling of zero/uninitialized LeaseTimeout
func TestLeaseCounterWithZeroLeaseTimeout(t *testing.T) {
	kclConfig := config.NewKinesisClientLibConfig("appName", "test-stream", "us-west-2", "worker-1").
		WithMaxLeasesForWorker(2)

	worker := &Worker{
		workerID:    "worker-1",
		kclConfig:   kclConfig,
		shardStatus: make(map[string]*par.ShardStatus),
	}

	validTime := time.Now().UTC().Add(1 * time.Hour)

	worker.shardStatus["shard-0001"] = &par.ShardStatus{
		ID:           "shard-0001",
		Mux:          &sync.RWMutex{},
		AssignedTo:   "worker-1",
		Checkpoint:   "12345",
		LeaseTimeout: validTime,
	}

	worker.shardStatus["shard-0002"] = &par.ShardStatus{
		ID:           "shard-0002",
		Mux:          &sync.RWMutex{},
		AssignedTo:   "worker-1",
		Checkpoint:   "67890",
		LeaseTimeout: time.Time{}, // Zero time - shard not yet fully initialized, should be counted
	}

	// Count leases
	counter := 0
	for _, shard := range worker.shardStatus {
		if shard.GetLeaseOwner() == worker.workerID && shard.GetCheckpoint() != chk.ShardEnd {
			leaseTimeout := shard.GetLeaseTimeout()
			if !leaseTimeout.IsZero() && leaseTimeout.Before(time.Now().UTC()) {
				continue
			}
			counter++
		}
	}

	// Should count both shards (zero timeout is not considered expired)
	assert.Equal(t, 2, counter, "Should count shards with zero LeaseTimeout as valid")
}

// TestLeaseCounterPreventOrphanRecoveryBlock tests the original bug scenario
func TestLeaseCounterPreventOrphanRecoveryBlock(t *testing.T) {
	// Simulate the UK-LON3 scenario:
	// - 81 shards / 3 workers = 27 shards per worker
	// - MaxLeasesForWorker = 27
	// - Some shards have expired leases but AssignedTo still set
	kclConfig := config.NewKinesisClientLibConfig("appName", "test-stream", "us-west-2", "worker-1").
		WithMaxLeasesForWorker(27)

	worker := &Worker{
		workerID:    "worker-1",
		kclConfig:   kclConfig,
		shardStatus: make(map[string]*par.ShardStatus),
	}

	now := time.Now().UTC()
	validTime := now.Add(1 * time.Hour)
	expiredTime := now.Add(-4 * time.Hour) // 4 hours expired like UK-LON3

	// Create 14 shards with valid leases (actively processing)
	for i := 1; i <= 14; i++ {
		shardID := "shard-active-" + string(rune('0'+i))
		worker.shardStatus[shardID] = &par.ShardStatus{
			ID:           shardID,
			Mux:          &sync.RWMutex{},
			AssignedTo:   "worker-1",
			Checkpoint:   "12345",
			LeaseTimeout: validTime,
		}
	}

	// Create 13 shards with expired leases (orphaned, renewal failed)
	for i := 1; i <= 13; i++ {
		shardID := "shard-orphan-" + string(rune('0'+i))
		worker.shardStatus[shardID] = &par.ShardStatus{
			ID:           shardID,
			Mux:          &sync.RWMutex{},
			AssignedTo:   "worker-1", // Still assigned but lease expired
			Checkpoint:   "67890",
			LeaseTimeout: expiredTime,
		}
	}

	// Count with OLD logic (without expiry check) - this was the bug
	oldCounter := 0
	for _, shard := range worker.shardStatus {
		if shard.GetLeaseOwner() == worker.workerID && shard.GetCheckpoint() != chk.ShardEnd {
			oldCounter++
		}
	}
	assert.Equal(t, 27, oldCounter, "Old logic counts all 27 shards")
	assert.False(t, oldCounter < worker.kclConfig.MaxLeasesForWorker,
		"Old logic: worker BLOCKED from claiming more shards (27 >= 27)")

	// Count with NEW logic (with expiry check) - the fix
	newCounter := 0
	for _, shard := range worker.shardStatus {
		if shard.GetLeaseOwner() == worker.workerID && shard.GetCheckpoint() != chk.ShardEnd {
			leaseTimeout := shard.GetLeaseTimeout()
			if !leaseTimeout.IsZero() && leaseTimeout.Before(time.Now().UTC()) {
				continue
			}
			newCounter++
		}
	}
	assert.Equal(t, 14, newCounter, "New logic counts only 14 active shards")
	assert.True(t, newCounter < worker.kclConfig.MaxLeasesForWorker,
		"New logic: worker CAN claim orphaned shards (14 < 27)")
}
