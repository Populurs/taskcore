package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	ModuleName         string // e.g. "port.scan"
	DefaultOSSBasePath string // e.g. "stream/naabu"
}

type TaskDealer struct {
	eb        eventbus.EventBus
	ossClient *oss.Client
	conf      *config.BaseConfig
	logger    *log.Logger
	handler   TaskHandler
	sem       chan struct{}
	dc        DealerConfig
	wg        sync.WaitGroup // 全局 WaitGroup，跟踪所有正在处理的 target

	// 并发监控指标
	activeTargets   atomic.Int64 // 当前正在处理的target数
	completedCount  atomic.Int64 // 已完成的target数（全局累计）
	peakConcurrent  atomic.Int64 // 峰值并发数
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

	return &TaskDealer{
		eb:        eb,
		ossClient: oss.NewClient(ossCfg),
		conf:      baseConf,
		logger:    logger,
		handler:   handler,
		sem:       make(chan struct{}, concurrency),
		dc:        dc,
	}, nil
}

// Close 等待所有正在处理的 target 完成，用于优雅关闭
func (d *TaskDealer) Close() {
	d.wg.Wait()
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

	d.logger.Info("start read OSS", zap.String("bucket", bucket), zap.String("object_key", objectKey))

	req := &oss.GetObjectRequest{
		Bucket: &bucket,
		Key:    &objectKey,
	}
	obj, err := d.ossClient.GetObject(ctx, req)
	if err != nil {
		return fmt.Errorf("get object error(bucket=%s,key=%s): %w", bucket, objectKey, err)
	}
	defer obj.Body.Close()

	hasRelay := len(d.conf.TopicWhoRelayOn) > 0 && !d.noEnabledDownstream(payload)
	metadata.EventType = d.dc.ModuleName
	inputShardPath := extractShardPath(objectKey)
	var shardSeq atomic.Int64

	flushRelayShard := func(buf []string) error {
		if len(buf) == 0 {
			return nil
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
		d.logger.Info("relay shard written to OSS", zap.String("key", obKey), zap.String("path", writtenPath), zap.Int("item_count", len(buf)))
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
				EnabledModules: payload.EnabledModules,
				PayloadVersion: payload.PayloadVersion,
			})
			if pubErr := d.eb.Publish(ctx, nextTopic, nextPayload, model.EventMetadataToMap(&metadata)); pubErr != nil {
				d.logger.Error("publish relay event error", zap.String("topic", nextTopic), zap.Error(pubErr))
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
			return fmt.Errorf("read line error: %w", readErr)
		}
	}
	d.logger.Info("read targets done", zap.String("oss_path", objectKey), zap.Int("target_count", len(targets)))

	startTime := time.Now()
	totalTargets := len(targets)

	// 在后台 goroutine 中分发并处理所有 target
	// 信号量 d.sem 是全局共享的，确保跨所有消息的总并发数受控
	go func() {
		var msgWg sync.WaitGroup

		for _, target := range targets {
			d.sem <- struct{}{} // 全局信号量，跨所有消息控制总并发数
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

				current := d.activeTargets.Add(1)
				// 更新峰值并发
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

				if !hasRelay || len(results) == 0 {
					return
				}

				cleaned := make([]string, 0, len(results))
				for _, r := range results {
					r = strings.TrimSpace(r)
					if r != "" {
						cleaned = append(cleaned, r)
					}
				}

				for i := 0; i < len(cleaned); i += d.conf.RelayShardMaxItems {
					end := i + d.conf.RelayShardMaxItems
					if end > len(cleaned) {
						end = len(cleaned)
					}
					if flushErr := flushRelayShard(cleaned[i:end]); flushErr != nil {
						d.logger.Error("flush relay shard error", zap.String("target", t), zap.Error(flushErr))
						return
					}
				}
			}(target)
		}

		msgWg.Wait()
		d.logger.Info("all targets processed",
			zap.String("event_id", eventID),
			zap.Int("total_targets", totalTargets),
			zap.Int64("global_active", d.activeTargets.Load()),
			zap.Int64("global_completed", d.completedCount.Load()),
			zap.Int64("peak_concurrent", d.peakConcurrent.Load()),
			zap.Int("sem_capacity", d.conf.Concurrent),
			zap.Duration("total_elapsed", time.Since(startTime)),
		)
	}()

	return nil
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