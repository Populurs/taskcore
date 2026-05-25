package controller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Populurs/eventbus"
	"github.com/Populurs/taskcore/config"
	"github.com/Populurs/taskcore/log"
	"github.com/Populurs/taskcore/model"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"go.uber.org/zap"
)

type TaskHandler interface {
	Handle(ctx context.Context, eventID, target string, payload model.TaskPayload, metadata model.EventMetadata) ([]string, error)
}

type DealerConfig struct {
	ModuleName         string
	DefaultOSSBasePath string
}

type TaskDealer struct {
	eb        eventbus.EventBus
	ossClient *oss.Client
	conf      *config.BaseConfig
	logger    *log.Logger
	handler   TaskHandler
	sem       chan struct{}
	dc        DealerConfig
	wg        sync.WaitGroup
	cancels   sync.Map
	tracker   *RedisTracker

	activeTargets  atomic.Int64
	completedCount atomic.Int64
	peakConcurrent atomic.Int64
}

func InitDealer(baseConf *config.BaseConfig, logger *log.Logger, handler TaskHandler, dc DealerConfig) (*TaskDealer, error) {
	eb, err := eventbus.NewRabbitMQEventBus(baseConf.Rabbitmq.Url+baseConf.Rabbitmq.Vhost, baseConf.Rabbitmq.PrefetchCount)
	if err != nil {
		return nil, err
	}

	ossCfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(baseConf.AliyunOSS.AccessKeyID, baseConf.AliyunOSS.AccessKeySecret)).
		WithRegion(baseConf.AliyunOSS.Region)
	if baseConf.AliyunOSS.Endpoint != "" {
		ossCfg = ossCfg.WithEndpoint(baseConf.AliyunOSS.Endpoint)
	}
	if baseConf.AliyunOSS.DisableSSL {
		ossCfg = ossCfg.WithDisableSSL(true)
	}
	if baseConf.AliyunOSS.UseInternalEndpoint {
		ossCfg = ossCfg.WithUseInternalEndpoint(true)
	}

	concurrency := baseConf.Concurrent
	if concurrency <= 0 {
		concurrency = 1
	}

	tracker, err := NewRedisTracker(baseConf.Data.Redis, dc.ModuleName, logger)
	if err != nil {
		logger.Warn("Redis tracker disabled", zap.Error(err))
		tracker = nil
	}

	return &TaskDealer{
		eb:        eb,
		ossClient: oss.NewClient(ossCfg),
		conf:      baseConf,
		logger:    logger,
		handler:   handler,
		sem:       make(chan struct{}, concurrency),
		dc:        dc,
		tracker:   tracker,
	}, nil
}

func (d *TaskDealer) Close() {
	d.wg.Wait()

	d.logger.Info("TaskDealer closing",
		zap.Int64("active_targets", d.activeTargets.Load()),
		zap.Int64("completed_targets", d.completedCount.Load()),
		zap.Int64("peak_concurrent", d.peakConcurrent.Load()),
	)
}

func (d *TaskDealer) SubscribeEvent() error {
	if err := d.eb.Subscribe(context.Background(), d.conf.TopicJobStart, d.newMessageHandler()); err != nil {
		return err
	}
	for _, relayTopic := range d.conf.TopicDependencyCompleted {
		if err := d.eb.Subscribe(context.Background(), relayTopic, d.newMessageHandler()); err != nil {
			return err
		}
	}
	if d.conf.TopicJobStop != "" {
		if err := d.eb.Subscribe(context.Background(), d.conf.TopicJobStop, d.newStopHandler()); err != nil {
			return err
		}
	}
	return nil
}

func (d *TaskDealer) newMessageHandler() eventbus.EventHandler {
	return func(eventID string, payload []byte, metadata map[string]string) error {
		err := d.handleEvent(eventID, payload, metadata)
		if err != nil {
			d.logger.Error("handle event failed",
				zap.String("event_id", eventID),
				zap.ByteString("payload", payload),
				zap.Any("metadata", metadata),
				zap.Error(err),
			)
			return nil
		}
		return nil
	}
}

func (d *TaskDealer) newStopHandler() eventbus.EventHandler {
	return func(eventID string, payload []byte, metadata map[string]string) error {
		eventMeta, err := model.MapToEventMetadata(metadata)
		if err != nil {
			d.logger.Error("stop handler: failed to parse metadata",
				zap.String("event_id", eventID),
				zap.Any("metadata", metadata),
				zap.Error(err),
			)
			return nil
		}

		if d.tracker != nil {
			if err := d.tracker.SetStopped(eventMeta.WorkTaskID); err != nil {
				d.logger.Error("stop handler: failed to sync stopped status",
					zap.String("event_id", eventID),
					zap.Uint32("work_task_id", eventMeta.WorkTaskID),
					zap.Error(err),
				)
			}
		}

		taskKey := model.TaskKey(eventMeta)
		if cancelFn, ok := d.cancels.LoadAndDelete(taskKey); ok {
			cancelFn.(context.CancelFunc)()
			d.logger.Info("task cancelled by stop event",
				zap.String("task_key", taskKey),
				zap.String("event_id", eventID),
			)
		} else {
			d.logger.Warn("stop event received but no active task found",
				zap.String("task_key", taskKey),
				zap.String("event_id", eventID),
			)
		}
		return nil
	}
}

func (d *TaskDealer) handleEvent(eventID string, payload []byte, metadata map[string]string) error {
	d.logger.Info("received message", zap.String("payload", string(payload)), zap.Any("metadata", metadata))

	pl := model.TaskPayload{}
	if err := json.Unmarshal(payload, &pl); err != nil {
		return fmt.Errorf("failed to parse payload: %w", err)
	}

	eventMeta, err := model.MapToEventMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	if err = d.readOSS(context.Background(), eventID, pl, *eventMeta); err != nil {
		return fmt.Errorf("failed to read OSS: %w", err)
	}
	return nil
}

func (d *TaskDealer) noEnabledDownstream(payload model.TaskPayload) bool {
	for _, nextTopic := range d.conf.TopicWhoRelayOn {
		lIndex := strings.LastIndex(nextTopic, ".")
		if lIndex <= 0 {
			d.logger.Warn("invalid topic", zap.String("topic", nextTopic))
			continue
		}
		downstreamModule := nextTopic[:lIndex]
		if payload.IsModuleEnabled(downstreamModule) {
			return false
		}
	}
	return true
}

func (d *TaskDealer) readOSS(ctx context.Context, eventID string, payload model.TaskPayload, metadata model.EventMetadata) error {
	bucket := d.conf.AliyunOSS.BucketName
	objectKey := strings.TrimSpace(payload.OssPath)
	if objectKey == "" {
		return fmt.Errorf("oss_path is empty")
	}

	taskKey := model.TaskKey(&metadata)
	ctx, cancel := context.WithCancel(ctx)
	d.cancels.Store(taskKey, cancel)

	isStopped := func() bool {
		return d.tracker != nil && d.tracker.IsStopped(metadata.WorkTaskID)
	}

	if isStopped() {
		cancel()
		d.cancels.Delete(taskKey)
		d.logger.Info("work task already stopped, skip event",
			zap.String("task_key", taskKey),
			zap.String("event_id", eventID),
		)
		return nil
	}

	d.logger.Info("start read OSS",
		zap.String("bucket", bucket),
		zap.String("object_key", objectKey),
		zap.String("task_key", taskKey),
	)

	if d.tracker != nil {
		if err := d.tracker.RegisterModule(metadata.WorkTaskID); err != nil {
			d.logger.Error("RedisTracker.RegisterModule failed",
				zap.Uint32("work_task_id", metadata.WorkTaskID),
				zap.Error(err),
			)
		}

		if !payload.IsHeadModule {
			if err := d.tracker.IncrTotal(metadata.WorkTaskID); err != nil {
				d.logger.Error("RedisTracker total update failed",
					zap.Uint32("work_task_id", metadata.WorkTaskID),
					zap.Error(err),
				)
			}
		}
	}

	req := &oss.GetObjectRequest{
		Bucket: &bucket,
		Key:    &objectKey,
	}
	obj, err := d.ossClient.GetObject(ctx, req)
	if err != nil {
		d.cancels.Delete(taskKey)
		cancel()
		return fmt.Errorf("get object error(bucket=%s,key=%s): %w", bucket, objectKey, err)
	}
	defer obj.Body.Close()

	hasRelay := len(d.conf.TopicWhoRelayOn) > 0 && !d.noEnabledDownstream(payload)
	metadata.EventType = d.dc.ModuleName
	metadata.SourceModule = d.dc.ModuleName
	inputShardPath := extractShardPath(objectKey)
	var shardSeq atomic.Int64
	var feedbackShardSeq atomic.Int64

	flushRelayShard := func(buf []string) error {
		if len(buf) == 0 {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isStopped() {
			return context.Canceled
		}

		localID := int(shardSeq.Add(1) - 1)
		shardPath := fmt.Sprintf("%s/%d", inputShardPath, localID)
		obKey := d.ossObjectKey(metadata, shardPath)
		content := strings.Join(buf, "\n")
		if content != "" && !strings.HasSuffix(content, "\n") {
			content += "\n"
		}

		writtenPath, err := d.writeOss(ctx, bucket, obKey, content)
		if err != nil {
			return err
		}
		d.logger.Info("relay shard written to OSS",
			zap.String("key", obKey),
			zap.String("path", writtenPath),
			zap.Int("item_count", len(buf)),
		)

		for _, nextTopic := range d.conf.TopicWhoRelayOn {
			lIndex := strings.LastIndex(nextTopic, ".")
			if lIndex <= 0 {
				d.logger.Warn("invalid topic", zap.String("topic", nextTopic))
				continue
			}
			downstreamModule := nextTopic[:lIndex]
			if !payload.IsModuleEnabled(downstreamModule) {
				continue
			}

			nextPayload, _ := json.Marshal(model.TaskPayload{
				OssPath:        obKey,
				Options:        payload.Options,
				IsHeadModule:   false,
				EnabledModules: payload.EnabledModules,
				PayloadVersion: payload.PayloadVersion,
			})
			if pubErr := d.eb.Publish(ctx, nextTopic, nextPayload, model.EventMetadataToMap(&metadata)); pubErr != nil {
				d.logger.Error("publish relay event error", zap.String("topic", nextTopic), zap.Error(pubErr))
				continue
			}
			if d.tracker != nil {
				if err := d.tracker.MarkDoneForDownstream(metadata.WorkTaskID, map[string]int64{downstreamModule: 1}); err != nil {
					d.logger.Error("RedisTracker.MarkDoneForDownstream failed",
						zap.Uint32("work_task_id", metadata.WorkTaskID),
						zap.String("downstream", downstreamModule),
						zap.Error(err),
					)
				}
			}
		}
		return nil
	}

	var targets []string
	reader := bufio.NewReader(obj.Body)
	for {
		line, readErr := reader.ReadString('\n')
		target := strings.TrimSpace(line)
		if target != "" {
			targets = append(targets, target)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			d.cancels.Delete(taskKey)
			cancel()
			return fmt.Errorf("read line error: %w", readErr)
		}
	}

	d.logger.Info("read targets done", zap.String("oss_path", objectKey), zap.Int("target_count", len(targets)))

	startTime := time.Now()
	totalTargets := len(targets)

	go func() {
		defer func() {
			d.cancels.Delete(taskKey)
			cancel()
		}()

		var msgWg sync.WaitGroup

	loop:
		for _, target := range targets {
			if ctx.Err() != nil || isStopped() {
				cancel()
				d.logger.Info("task cancelled, skip remaining targets", zap.String("task_key", taskKey))
				break
			}

			select {
			case d.sem <- struct{}{}:
			case <-ctx.Done():
				d.logger.Info("task cancelled while waiting for semaphore", zap.String("task_key", taskKey))
				break loop
			}

			msgWg.Add(1)
			d.wg.Add(1)
			go func(t string) {
				defer func() {
					<-d.sem
					current := d.activeTargets.Add(-1)
					completed := d.completedCount.Add(1)
					d.logger.Info("target done",
						zap.String("target", t),
						zap.Int64("active_targets", current),
						zap.Int64("completed", completed),
						zap.Int("total", totalTargets),
						zap.Duration("elapsed", time.Since(startTime)),
					)
					msgWg.Done()
					d.wg.Done()
				}()

				if ctx.Err() != nil || isStopped() {
					cancel()
					return
				}

				current := d.activeTargets.Add(1)
				for {
					peak := d.peakConcurrent.Load()
					if current <= peak || d.peakConcurrent.CompareAndSwap(peak, current) {
						break
					}
				}

				d.logger.Info("start handle target",
					zap.String("target", t),
					zap.Int64("active_targets", current),
					zap.Int64("peak_concurrent", d.peakConcurrent.Load()),
					zap.Int("sem_capacity", d.conf.Concurrent),
				)

				results, handleErr := d.handler.Handle(ctx, eventID, t, payload, metadata)
				if handleErr != nil {
					d.logger.Error("handle target error", zap.String("target", t), zap.Error(handleErr))
					return
				}
				d.logger.Info("handle target done",
					zap.String("target", t),
					zap.Int("result_count", len(results)),
					zap.Any("results", results),
				)

				if len(results) == 0 {
					return
				}
				if ctx.Err() != nil || isStopped() {
					cancel()
					return
				}

				cleaned := make([]string, 0, len(results))
				for _, r := range results {
					r = strings.TrimSpace(r)
					if r != "" {
						cleaned = append(cleaned, r)
					}
				}
				if len(cleaned) == 0 {
					return
				}

				for i := 0; i < len(cleaned); i += d.conf.RelayShardMaxItems {
					end := i + d.conf.RelayShardMaxItems
					if end > len(cleaned) {
						end = len(cleaned)
					}
					shardResults := append([]string(nil), cleaned[i:end]...)
					if hasRelay {
						if flushErr := flushRelayShard(shardResults); flushErr != nil {
							d.logger.Error("flush relay shard error", zap.String("target", t), zap.Error(flushErr))
							return
						}
					}
					if d.conf.EnableResultFeedback && len(d.conf.TopicResultFeedback) > 0 {
						feedbackShardID := int(feedbackShardSeq.Add(1) - 1)
						d.sendResultFeedback(ctx, metadata, shardResults, feedbackShardID)
					}
				}
			}(target)
		}

		msgWg.Wait()
		d.logger.Info("all targets processed, this event accomplished",
			zap.String("event_id", eventID),
			zap.Int("total_targets", totalTargets),
			zap.Int64("global_active", d.activeTargets.Load()),
			zap.Int64("global_completed", d.completedCount.Load()),
			zap.Int64("peak_concurrent", d.peakConcurrent.Load()),
			zap.Int("sem_capacity", d.conf.Concurrent),
			zap.Duration("total_elapsed", time.Since(startTime)),
		)

		if isStopped() {
			d.logger.Info("work task stopped, skip shard completion sync",
				zap.String("event_id", eventID),
				zap.Uint32("work_task_id", metadata.WorkTaskID),
			)
			return
		}

		if d.tracker != nil {
			if _, shardErr := d.tracker.OnShardDone(metadata.WorkTaskID); shardErr != nil {
				d.logger.Error("RedisTracker.OnShardDone failed",
					zap.Uint32("work_task_id", metadata.WorkTaskID),
					zap.Error(shardErr),
				)
			}

			if !isStopped() && d.tracker.IsCurrentModuleDone(metadata.WorkTaskID, payload) {
				if err := d.tracker.SetCompleted(metadata.WorkTaskID); err != nil {
					d.logger.Error("failed to mark current module completed",
						zap.Uint32("work_task_id", metadata.WorkTaskID),
						zap.Error(err),
					)
				}
			}
		}
	}()

	return nil
}

func (d *TaskDealer) sendResultFeedback(ctx context.Context, metadata model.EventMetadata, result []string, shardID int) {
	var ossPath string
	if len(d.conf.TopicResultFeedback) == 0 {
		d.logger.Debug("result feedback disabled, skip sending",
			zap.String("task_key", model.TaskKey(&metadata)),
		)
		return
	}

	if len(result) == 0 {
		d.logger.Debug("empty result to send",
			zap.String("task_key", model.TaskKey(&metadata)),
		)
		return
	}

	taskKey := model.ResultKeyFromMetadata(&metadata)
	if d.conf.AliyunOSS.BasePath == "" {
		ossPath = filepath.Join(d.dc.DefaultOSSBasePath, taskKey, fmt.Sprintf("feedback_%d.json", shardID))
	} else {
		ossPath = filepath.Join(d.conf.AliyunOSS.BasePath, taskKey, fmt.Sprintf("feedback_%d.json", shardID))
	}

	resultBuf, err := model.JoinJSONStrings(result)
	if err != nil {
		d.logger.Error("join json strings error",
			zap.String("task_key", taskKey),
			zap.Any("result", result),
			zap.Error(err),
		)
		return
	}

	for _, topic := range d.conf.TopicResultFeedback {
		if _, putossErr := d.ossClient.PutObject(ctx, &oss.PutObjectRequest{
			Bucket: oss.Ptr(d.conf.AliyunOSS.BucketName),
			Key:    oss.Ptr(ossPath),
			Body:   bytes.NewReader(resultBuf),
		}); putossErr != nil {
			d.logger.Error("write feedback result to oss error",
				zap.String("topic", topic),
				zap.String("oss path", ossPath),
				zap.Error(putossErr),
			)
		} else {
			if pubErr := d.eb.Publish(ctx, topic, []byte(ossPath), model.EventMetadataToMap(&metadata)); pubErr != nil {
				d.logger.Error("publish result feedback error",
					zap.String("topic", topic),
					zap.String("task_key", taskKey),
					zap.Error(pubErr),
				)
			} else {
				d.logger.Info("result feedback published successfully",
					zap.String("topic", topic),
					zap.String("task_key", taskKey),
				)
			}
		}
	}
}

func (d *TaskDealer) writeOss(ctx context.Context, bucket, objectKey, content string) (string, error) {
	req := &oss.PutObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(objectKey),
		Body:   strings.NewReader(content),
	}
	_, err := d.ossClient.PutObject(ctx, req)
	if err != nil {
		return "", err
	}

	if d.conf.AliyunOSS.BucketURL != "" {
		base := strings.TrimRight(d.conf.AliyunOSS.BucketURL, "/")
		return fmt.Sprintf("%s/%s", base, objectKey), nil
	}
	return objectKey, nil
}

func (d *TaskDealer) ossObjectKey(metadata model.EventMetadata, shardPath string) string {
	base := strings.Trim(strings.TrimSpace(d.conf.AliyunOSS.BasePath), "/")
	if base == "" {
		base = d.dc.DefaultOSSBasePath
	}
	return fmt.Sprintf("%s/%d-%d-%d-%d/%s", base, metadata.TaskID, metadata.WorkTaskID, metadata.TenantID, metadata.UserID, shardPath)
}

func extractShardPath(ossPath string) string {
	s := strings.TrimSuffix(ossPath, "_targets")

	parts := strings.Split(s, "/")
	for i, part := range parts {
		if strings.Count(part, "-") == 3 {
			if i+1 < len(parts) {
				return strings.Join(parts[i+1:], "/")
			}
			return "0"
		}
	}

	lastSep := strings.LastIndexAny(s, "_-")
	if lastSep >= 0 && lastSep < len(s)-1 {
		if _, err := strconv.Atoi(s[lastSep+1:]); err == nil {
			return s[lastSep+1:]
		}
	}
	return "0"
}
