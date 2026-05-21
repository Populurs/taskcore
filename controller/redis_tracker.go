package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Populurs/taskcore/config"
	"github.com/Populurs/taskcore/log"
	"github.com/Populurs/taskcore/model"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const defaultTTL = 24 * time.Hour

// RedisTracker updates Redis keys that the scheduler polls.
// Key format mirrors scheduler/internal/rediskeys exactly.
type RedisTracker struct {
	rdb        *redis.Client
	moduleName string
	ttl        time.Duration
	logger     *log.Logger
	registered sync.Map
}

// NewRedisTracker creates a tracker. Returns (nil, nil) when addr is empty.
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
		moduleName: normalizeModuleName(moduleName),
		ttl:        ttl,
		logger:     logger,
	}, nil
}

func (rt *RedisTracker) RegisterModule(workTaskID uint32) error {
	ctx := context.Background()
	pipe := rt.rdb.Pipeline()

	modulesKey := rt.modulesKey(workTaskID)
	statusKey := rt.statusKey(workTaskID)
	pipe.SAdd(ctx, modulesKey, rt.moduleName)
	pipe.Expire(ctx, modulesKey, rt.ttl)

	if _, loaded := rt.registered.LoadOrStore(workTaskID, true); loaded {
		pipe.Expire(ctx, statusKey, rt.ttl)
	} else {
		pipe.Set(ctx, statusKey, "running", rt.ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		rt.logger.Error("RedisTracker.RegisterModule failed",
			zap.Uint32("work_task_id", workTaskID),
			zap.Error(err),
		)
	}
	return err
}

func (rt *RedisTracker) IncrTotal(workTaskID uint32) error {
	return rt.incrTotal(workTaskID, 1)
}

func (rt *RedisTracker) incrTotal(workTaskID uint32, delta int64) error {
	ctx := context.Background()
	pipe := rt.rdb.Pipeline()

	modulesKey := rt.modulesKey(workTaskID)
	statusKey := rt.statusKey(workTaskID)
	totalKey := rt.shardsTotalKey(workTaskID)

	pipe.IncrBy(ctx, totalKey, delta)
	pipe.Expire(ctx, totalKey, rt.ttl)
	pipe.Expire(ctx, modulesKey, rt.ttl)
	pipe.Expire(ctx, statusKey, rt.ttl)

	_, err := pipe.Exec(ctx)
	return err
}

func (rt *RedisTracker) OnShardDone(workTaskID uint32) (int64, error) {
	ctx := context.Background()
	pipe := rt.rdb.Pipeline()

	modulesKey := rt.modulesKey(workTaskID)
	statusKey := rt.statusKey(workTaskID)
	doneKey := rt.shardsDoneKey(workTaskID)

	incrCmd := pipe.Incr(ctx, doneKey)
	pipe.Expire(ctx, doneKey, rt.ttl)
	pipe.Expire(ctx, modulesKey, rt.ttl)
	pipe.Expire(ctx, statusKey, rt.ttl)

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incrCmd.Val(), nil
}

func (rt *RedisTracker) IsCurrentModuleDone(workTaskID uint32, payload model.TaskPayload) bool {
	ctx := context.Background()
	doneVal, totalVal, err := rt.loadCurrentProgress(ctx, workTaskID)
	if err != nil {
		return false
	}

	if payload.IsHeadModule {
		return totalVal > 0 && doneVal >= totalVal
	}

	expectedTotal, allDone, err := rt.expectedTotalFromUpstreams(ctx, workTaskID)
	if err != nil || !allDone {
		return false
	}

	return totalVal > 0 && totalVal >= expectedTotal && doneVal >= totalVal
}

func (rt *RedisTracker) SetStatus(workTaskID uint32, status string) error {
	ctx := context.Background()
	pipe := rt.rdb.Pipeline()

	modulesKey := rt.modulesKey(workTaskID)
	statusKey := rt.statusKey(workTaskID)

	pipe.Set(ctx, statusKey, status, rt.ttl)
	pipe.Expire(ctx, modulesKey, rt.ttl)

	_, err := pipe.Exec(ctx)
	return err
}

func (rt *RedisTracker) SetStopped(workTaskID uint32) error {
	ctx := context.Background()

	statusKey := rt.statusKey(workTaskID)
	status, err := rt.rdb.Get(ctx, statusKey).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	pipe := rt.rdb.Pipeline()

	modulesKey := rt.modulesKey(workTaskID)
	stopKey := rt.stopKey(workTaskID)

	pipe.Set(ctx, stopKey, "1", rt.ttl)
	if status != "completed" {
		pipe.Set(ctx, statusKey, "stopped", rt.ttl)
	} else {
		pipe.Expire(ctx, statusKey, rt.ttl)
	}
	pipe.Expire(ctx, modulesKey, rt.ttl)

	_, err = pipe.Exec(ctx)
	return err
}

func (rt *RedisTracker) IsStopped(workTaskID uint32) bool {
	ctx := context.Background()
	exists, err := rt.rdb.Exists(ctx, rt.stopKey(workTaskID)).Result()
	if err != nil {
		return false
	}
	return exists > 0
}

func (rt *RedisTracker) LoadUpstreams(workTaskID uint32, module string) ([]string, error) {
	ctx := context.Background()
	module = normalizeModuleName(module)
	if module == "" {
		return nil, nil
	}

	items, err := rt.rdb.SMembers(ctx, rt.upstreamsKey(workTaskID, module)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(items))
	upstreams := make([]string, 0, len(items))
	for _, item := range items {
		moduleName := normalizeModuleName(item)
		if moduleName == "" {
			continue
		}
		if _, ok := seen[moduleName]; ok {
			continue
		}
		seen[moduleName] = struct{}{}
		upstreams = append(upstreams, moduleName)
	}
	return upstreams, nil
}

func (rt *RedisTracker) MarkDoneForDownstream(workTaskID uint32, shardCounts map[string]int64) error {
	ctx := context.Background()
	var firstErr error

	for downstream, count := range shardCounts {
		if count <= 0 {
			continue
		}

		downstream = normalizeModuleName(downstream)
		if downstream == "" {
			continue
		}

		pipe := rt.rdb.Pipeline()
		upstreamsKey := rt.upstreamsKey(workTaskID, downstream)
		flagKey := rt.upstreamDoneKey(workTaskID, downstream, rt.moduleName)
		modulesKey := rt.modulesKey(workTaskID)

		pipe.SAdd(ctx, upstreamsKey, rt.moduleName)
		pipe.Expire(ctx, upstreamsKey, rt.ttl)
		pipe.IncrBy(ctx, flagKey, count)
		pipe.Expire(ctx, flagKey, rt.ttl)
		pipe.Expire(ctx, modulesKey, rt.ttl)

		if _, err := pipe.Exec(ctx); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			rt.logger.Error("RedisTracker.MarkDoneForDownstream failed",
				zap.Uint32("work_task_id", workTaskID),
				zap.String("downstream", downstream),
				zap.Int64("count", count),
				zap.Error(err),
			)
		}
	}

	return firstErr
}

func (rt *RedisTracker) loadCurrentProgress(ctx context.Context, workTaskID uint32) (done int64, total int64, err error) {
	pipe := rt.rdb.Pipeline()
	doneCmd := pipe.Get(ctx, rt.shardsDoneKey(workTaskID))
	totalCmd := pipe.Get(ctx, rt.shardsTotalKey(workTaskID))

	if _, err = pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, 0, err
	}

	doneVal, err := parseInt64Cmd(doneCmd)
	if err != nil {
		return 0, 0, err
	}

	totalVal, err := parseInt64Cmd(totalCmd)
	if err != nil {
		return 0, 0, err
	}

	return doneVal, totalVal, nil
}

func (rt *RedisTracker) expectedTotalFromUpstreams(ctx context.Context, workTaskID uint32) (int64, bool, error) {
	upstreams, err := rt.LoadUpstreams(workTaskID, rt.moduleName)
	if err != nil {
		return 0, false, err
	}
	if len(upstreams) == 0 {
		return 0, false, nil
	}

	pipe := rt.rdb.Pipeline()
	statusCmds := make([]*redis.StringCmd, 0, len(upstreams))
	doneCmds := make([]*redis.StringCmd, 0, len(upstreams))
	for _, upstream := range upstreams {
		statusCmds = append(statusCmds, pipe.Get(ctx, rt.upstreamStatusKey(workTaskID, upstream)))
		doneCmds = append(doneCmds, pipe.Get(ctx, rt.upstreamDoneKey(workTaskID, rt.moduleName, upstream)))
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, false, err
	}

	var sum int64
	for i := range upstreams {
		status, err := statusCmds[i].Result()
		if err == redis.Nil || err != nil || status != "completed" {
			return 0, false, nil
		}

		val, err := doneCmds[i].Result()
		if err == redis.Nil {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, false, err
		}
		sum += n
	}

	return sum, true, nil
}

func parseInt64Cmd(cmd *redis.StringCmd) (int64, error) {
	val, err := cmd.Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

func normalizeModuleName(name string) string {
	return strings.TrimSpace(strings.ToLower(name))
}

func (rt *RedisTracker) modulesKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:modules", workTaskID)
}

func (rt *RedisTracker) statusKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:%s:status", workTaskID, rt.moduleName)
}

func (rt *RedisTracker) stopKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:stop", workTaskID)
}

func (rt *RedisTracker) upstreamsKey(workTaskID uint32, module string) string {
	return fmt.Sprintf("wt:%d:%s:upstreams", workTaskID, normalizeModuleName(module))
}

func (rt *RedisTracker) upstreamStatusKey(workTaskID uint32, upstream string) string {
	return fmt.Sprintf("wt:%d:%s:status", workTaskID, normalizeModuleName(upstream))
}

func (rt *RedisTracker) shardsTotalKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:%s:shards:total", workTaskID, rt.moduleName)
}

func (rt *RedisTracker) shardsDoneKey(workTaskID uint32) string {
	return fmt.Sprintf("wt:%d:%s:shards:done", workTaskID, rt.moduleName)
}

func (rt *RedisTracker) upstreamDoneKey(workTaskID uint32, downstream, upstream string) string {
	return fmt.Sprintf("wt:%d:%s:upstream_done:%s", workTaskID, normalizeModuleName(downstream), normalizeModuleName(upstream))
}
