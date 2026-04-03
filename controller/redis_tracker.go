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
// For downstream modules: completed when done == total AND at least one upstream_done marker exists.
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
	return rt.hasUpstreamDone(workTaskID)
}

// SetStatus sets the module's status key.
func (rt *RedisTracker) SetStatus(workTaskID uint32, status string) error {
	ctx := context.Background()
	return rt.rdb.Set(ctx, rt.statusKey(workTaskID), status, rt.ttl).Err()
}

// MarkUpstreamDone is called by the upstream module after all its targets have been processed.
// It sets an upstream_done flag on each downstream module, then checks if the downstream
// is already complete (done == total) and marks it completed if so.
func (rt *RedisTracker) MarkUpstreamDone(workTaskID uint32, downstreamModules []string) error {
	ctx := context.Background()

	for _, downstream := range downstreamModules {
		flagKey := rt.upstreamDoneKey(workTaskID, downstream, rt.moduleName)
		if err := rt.rdb.Set(ctx, flagKey, "1", rt.ttl).Err(); err != nil {
			rt.logger.Error("RedisTracker.MarkUpstreamDone set flag failed",
				zap.Uint32("work_task_id", workTaskID),
				zap.String("downstream", downstream),
				zap.Error(err),
			)
			continue
		}

		// Check if downstream is now complete (done == total and upstream_done exists)
		rt.tryCompleteDownstream(ctx, workTaskID, downstream)
	}
	return nil
}

// tryCompleteDownstream checks if a downstream module has done==total and sets completed if so.
func (rt *RedisTracker) tryCompleteDownstream(ctx context.Context, workTaskID uint32, downstream string) {
	totalStr, err := rt.rdb.Get(ctx, fmt.Sprintf("wt:%d:%s:shards:total", workTaskID, downstream)).Result()
	if err != nil {
		return
	}
	doneStr, err := rt.rdb.Get(ctx, fmt.Sprintf("wt:%d:%s:shards:done", workTaskID, downstream)).Result()
	if err != nil {
		return
	}

	total, _ := strconv.ParseInt(totalStr, 10, 64)
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

// hasUpstreamDone checks if at least one upstream_done marker exists for this module.
func (rt *RedisTracker) hasUpstreamDone(workTaskID uint32) (bool, error) {
	ctx := context.Background()
	pattern := fmt.Sprintf("wt:%d:%s:upstream_done:*", workTaskID, rt.moduleName)

	var cursor uint64
	keys, cursor, err := rt.rdb.Scan(ctx, cursor, pattern, 1).Result()
	if err != nil {
		return false, err
	}
	return len(keys) > 0 || cursor != 0, nil
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
