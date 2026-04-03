package controller

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Populurs/taskcore/config"
	"github.com/Populurs/taskcore/log"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const defaultTTL = 24 * time.Hour

// RedisTracker updates Redis keys that the scheduler's monitWorkTaskStatus polls.
// Key format mirrors scheduler/internal/rediskeys exactly.
type RedisTracker struct {
	rdb        *redis.Client
	moduleName string // e.g. "enterprise.scan"
	ttl        time.Duration
	logger     *log.Logger
	registered sync.Map // workTaskID → bool (prevents duplicate RegisterModule)
}

// NewRedisTracker creates a tracker. Returns (nil, nil) when addr is empty (disabled).
// On connection failure returns (nil, err) — caller should log and continue without tracker.
func NewRedisTracker(conf config.RedisConfig, moduleName string, logger *log.Logger) (*RedisTracker, error) {
	if conf.Addr == "" {
		return nil, nil
	}

	ttl := conf.JobTTL
	if ttl <= 0 {
		ttl = defaultTTL
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         conf.Addr,
		Password:     conf.Password,
		DB:           conf.DB,
		ReadTimeout:  conf.ReadTimeout,
		WriteTimeout: conf.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisTracker{
		rdb:        rdb,
		moduleName: moduleName,
		ttl:        ttl,
		logger:     logger,
	}, nil
}

// RegisterModule idempotently adds this module to the work-task's module set
// and sets status=running (only on first call per workTaskID).
func (rt *RedisTracker) RegisterModule(workTaskID uint32) error {
	if _, loaded := rt.registered.LoadOrStore(workTaskID, true); loaded {
		return nil
	}

	ctx := context.Background()
	pipe := rt.rdb.Pipeline()

	modulesKey := rt.modulesKey(workTaskID)
	pipe.SAdd(ctx, modulesKey, rt.moduleName)
	pipe.Expire(ctx, modulesKey, rt.ttl)
	pipe.Set(ctx, rt.statusKey(workTaskID), "running", rt.ttl)

	_, err := pipe.Exec(ctx)
	if err != nil {
		rt.logger.Error("RedisTracker.RegisterModule failed",
			zap.Uint32("work_task_id", workTaskID),
			zap.Error(err),
		)
	}
	return err
}

// IsHeadModule checks whether shards:total was pre-set by the scheduler.
// Head modules have total > 0 before the agent touches them.
func (rt *RedisTracker) IsHeadModule(workTaskID uint32) (bool, error) {
	ctx := context.Background()
	val, err := rt.rdb.Get(ctx, rt.shardsTotalKey(workTaskID)).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	n, _ := strconv.Atoi(val)
	return n > 0, nil
}

// IncrTotal increments shards:total for a downstream module (called once per incoming shard).
func (rt *RedisTracker) IncrTotal(workTaskID uint32) error {
	ctx := context.Background()
	pipe := rt.rdb.Pipeline()
	key := rt.shardsTotalKey(workTaskID)
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, rt.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// OnShardDone increments shards:done and checks whether the module is complete.
// For head modules: completed when done == total.
// For downstream modules: completed when done == total AND all upstreams have marked done
// AND the sum of upstream shard counts equals total (ensuring all shards have arrived).
func (rt *RedisTracker) OnShardDone(workTaskID uint32) (completed bool, err error) {
	ctx := context.Background()

	// INCR done
	pipe := rt.rdb.Pipeline()
	doneKey := rt.shardsDoneKey(workTaskID)
	incrCmd := pipe.Incr(ctx, doneKey)
	pipe.Expire(ctx, doneKey, rt.ttl)
	if _, err = pipe.Exec(ctx); err != nil {
		return false, err
	}
	done := incrCmd.Val()

	// GET total
	totalStr, err := rt.rdb.Get(ctx, rt.shardsTotalKey(workTaskID)).Result()
	if err != nil {
		return false, err
	}
	total, _ := strconv.ParseInt(totalStr, 10, 64)
	if total <= 0 || done < total {
		return false, nil
	}

	// done >= total — check if head module or upstream_done exists
	isHead, err := rt.IsHeadModule(workTaskID)
	if err != nil {
		return false, err
	}
	if isHead {
		return true, nil
	}
	return rt.allUpstreamsDone(workTaskID)
}

// SetStatus sets the module's status key.
func (rt *RedisTracker) SetStatus(workTaskID uint32, status string) error {
	ctx := context.Background()
	return rt.rdb.Set(ctx, rt.statusKey(workTaskID), status, rt.ttl).Err()
}

// MarkUpstreamDone is called by the upstream module after all its targets have been processed.
// It sets an upstream_done flag on each downstream module with the shard count as value,
// then checks if the downstream is already complete and marks it completed if so.
// shardCounts maps downstream module name → number of shards this upstream relayed to it.
func (rt *RedisTracker) MarkUpstreamDone(workTaskID uint32, shardCounts map[string]int64) error {
	ctx := context.Background()

	for downstream, count := range shardCounts {
		flagKey := rt.upstreamDoneKey(workTaskID, downstream, rt.moduleName)
		if err := rt.rdb.Set(ctx, flagKey, strconv.FormatInt(count, 10), rt.ttl).Err(); err != nil {
			rt.logger.Error("RedisTracker.MarkUpstreamDone set flag failed",
				zap.Uint32("work_task_id", workTaskID),
				zap.String("downstream", downstream),
				zap.Error(err),
			)
			continue
		}

		// Check if downstream is now complete
		rt.tryCompleteDownstream(ctx, workTaskID, downstream)
	}
	return nil
}

// tryCompleteDownstream checks if a downstream module has:
// 1. All upstreams marked done
// 2. Sum of upstream shard counts == shards:total (all shards arrived)
// 3. shards:done == shards:total (all shards processed)
// Only then marks the downstream as completed.
func (rt *RedisTracker) tryCompleteDownstream(ctx context.Context, workTaskID uint32, downstream string) {
	// 1. Check all upstreams are done and get expected total
	expectedTotal, allDone, err := rt.expectedTotalFromUpstreams(ctx, workTaskID, downstream)
	if err != nil || !allDone {
		return
	}

	// 2. Check shards:total matches expected (all shards have arrived at downstream)
	totalStr, err := rt.rdb.Get(ctx, fmt.Sprintf("wt:%d:%s:shards:total", workTaskID, downstream)).Result()
	if err != nil {
		return
	}
	total, _ := strconv.ParseInt(totalStr, 10, 64)
	if total < expectedTotal {
		return // not all shards have been received yet
	}

	// 3. Check shards:done == shards:total (all shards processed)
	doneStr, err := rt.rdb.Get(ctx, fmt.Sprintf("wt:%d:%s:shards:done", workTaskID, downstream)).Result()
	if err != nil {
		return
	}
	done, _ := strconv.ParseInt(doneStr, 10, 64)
	if total <= 0 || done < total {
		return
	}

	// Check status — only complete if still running
	statusKey := fmt.Sprintf("wt:%d:%s:status", workTaskID, downstream)
	status, err := rt.rdb.Get(ctx, statusKey).Result()
	if err != nil || status != "running" {
		return
	}

	if err := rt.rdb.Set(ctx, statusKey, "completed", rt.ttl).Err(); err != nil {
		rt.logger.Error("RedisTracker.tryCompleteDownstream failed",
			zap.Uint32("work_task_id", workTaskID),
			zap.String("downstream", downstream),
			zap.Error(err),
		)
	}
}

// RegisterUpstream registers the current module as an upstream of a downstream module.
// Called when first relaying a shard to a downstream, so the downstream knows all its upstreams.
func (rt *RedisTracker) RegisterUpstream(workTaskID uint32, downstream string) error {
	ctx := context.Background()
	key := rt.upstreamsKey(workTaskID, downstream)
	pipe := rt.rdb.Pipeline()
	pipe.SAdd(ctx, key, rt.moduleName)
	pipe.Expire(ctx, key, rt.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// allUpstreamsDone checks that ALL registered upstreams have marked done for this module.
// Returns false if no upstreams are registered (safety: don't accidentally mark complete).
func (rt *RedisTracker) allUpstreamsDone(workTaskID uint32) (bool, error) {
	ctx := context.Background()
	upstreamsKey := rt.upstreamsKey(workTaskID, rt.moduleName)
	upstreams, err := rt.rdb.SMembers(ctx, upstreamsKey).Result()
	if err != nil {
		return false, err
	}
	if len(upstreams) == 0 {
		return false, nil
	}
	for _, up := range upstreams {
		flagKey := rt.upstreamDoneKey(workTaskID, rt.moduleName, up)
		exists, err := rt.rdb.Exists(ctx, flagKey).Result()
		if err != nil {
			return false, err
		}
		if exists == 0 {
			return false, nil
		}
	}
	return true, nil
}

// allUpstreamsDoneFor is like allUpstreamsDone but for an arbitrary downstream module name.
func (rt *RedisTracker) allUpstreamsDoneFor(ctx context.Context, workTaskID uint32, downstream string) (bool, error) {
	upstreamsKey := rt.upstreamsKey(workTaskID, downstream)
	upstreams, err := rt.rdb.SMembers(ctx, upstreamsKey).Result()
	if err != nil {
		return false, err
	}
	if len(upstreams) == 0 {
		return false, nil
	}
	for _, up := range upstreams {
		flagKey := rt.upstreamDoneKey(workTaskID, downstream, up)
		exists, err := rt.rdb.Exists(ctx, flagKey).Result()
		if err != nil {
			return false, err
		}
		if exists == 0 {
			return false, nil
		}
	}
	return true, nil
}

// expectedTotalFromUpstreams sums the shard counts stored in all upstream_done keys for a downstream.
// Returns (sum, true) if all upstreams are done, or (0, false) if any upstream hasn't marked done yet.
func (rt *RedisTracker) expectedTotalFromUpstreams(ctx context.Context, workTaskID uint32, downstream string) (int64, bool, error) {
	upstreamsKey := rt.upstreamsKey(workTaskID, downstream)
	upstreams, err := rt.rdb.SMembers(ctx, upstreamsKey).Result()
	if err != nil {
		return 0, false, err
	}
	if len(upstreams) == 0 {
		return 0, false, nil
	}
	var sum int64
	for _, up := range upstreams {
		flagKey := rt.upstreamDoneKey(workTaskID, downstream, up)
		val, err := rt.rdb.Get(ctx, flagKey).Result()
		if err == redis.Nil {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		n, _ := strconv.ParseInt(val, 10, 64)
		sum += n
	}
	return sum, true, nil
}

// Redis key helpers — format matches scheduler/internal/rediskeys exactly.

func (rt *RedisTracker) modulesKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:modules", workTaskID)
}

func (rt *RedisTracker) statusKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:%s:status", workTaskID, rt.moduleName)
}

func (rt *RedisTracker) shardsTotalKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:%s:shards:total", workTaskID, rt.moduleName)
}

func (rt *RedisTracker) shardsDoneKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:%s:shards:done", workTaskID, rt.moduleName)
}

func (rt *RedisTracker) upstreamDoneKey(workTaskID uint32, downstream, upstream string) string {
	return fmt.Sprintf("wt:%d:%s:upstream_done:%s", workTaskID, downstream, upstream)
}

func (rt *RedisTracker) upstreamsKey(workTaskID uint32, downstream string) string {
	return fmt.Sprintf("wt:%d:%s:upstreams", workTaskID, downstream)
}
