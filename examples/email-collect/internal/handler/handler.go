package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Populurs/taskcore/config"
	"github.com/Populurs/taskcore/log"
	"github.com/Populurs/taskcore/model"
	"github.com/Populurs/taskcore/repository"
	"github.com/PuerkitoBio/goquery"
	"github.com/robfig/cron/v3"
)

// Handler 邮箱收集处理器
type Handler struct {
	logger *log.Logger
	repo   *repository.EmailRepository
	config *config.Config

	// 数据收集器
	githubCollector *GitHubCollector
	bingCollector   *BingCollector
	sogouCollector  *SogouCollector
	zerozoneCollector *ZerozoneCollector

	// 限制器
	rateLimiter *RateLimiter
}

// NewHandler 创建邮箱收集处理器
func NewHandler(logger *log.Logger, repo *repository.EmailRepository, cfg *config.Config) *Handler {
	h := &Handler{
		logger: logger,
		repo:   repo,
		config: cfg,

		rateLimiter: NewRateLimiter(5, 1*time.Second), // 5 QPS
	}

	// 初始化数据收集器
	if cfg.EmailCollect.GitHub.Enabled {
		h.githubCollector = NewGitHubCollector(cfg, logger)
	}
	if cfg.EmailCollect.Bing.Enabled {
		h.bingCollector = NewBingCollector(cfg, logger)
	}
	if cfg.EmailCollect.Sogou.Enabled {
		h.sogouCollector = NewSogouCollector(cfg, logger)
	}
	if cfg.EmailCollect.Zerozone.Enabled {
		h.zerozoneCollector = NewZerozoneCollector(cfg, logger)
	}

	return h
}

// Handle 实现 TaskCore 的 TaskHandler 接口
func (h *Handler) Handle(ctx context.Context, eventID, target string, payload model.TaskPayload, metadata model.EventMetadata) ([]string, error) {
	// 检查取消信号
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 模块启用检查
	if !payload.IsModuleEnabled("email_collect") {
		h.logger.Info("email_collect module disabled",
			"event_id", eventID,
			"target", target)
		return nil, nil
	}

	// 解析选项
	taskOpts, err := model.ResolveOptions[model.EmailTaskOptions](payload, "email_collect", model.ApplyEmailOptions)
	if err != nil {
		return nil, fmt.Errorf("resolve options failed: %w", err)
	}

	h.logger.Info("Start collecting emails",
		"event_id", eventID,
		"target", target,
		"options", taskOpts)

	// 收集邮箱
	emails := h.collectEmails(ctx, target, taskOpts)

	// 异步保存结果
	resultChan := make(chan []model.EmailPersistPayload, 1)
	go func() {
		payloads := make([]model.EmailPersistPayload, 0, len(emails))
		for _, email := range emails {
			payloads = append(payloads, model.EmailPersistPayload{
				Email:  email,
				Target: target,
			})
		}

		err := h.repo.SaveResults(ctx, &metadata, resultChan)
		if err != nil {
			h.logger.Error("save failed",
				"event_id", eventID,
				"error", err)
		}
	}()

	// 返回结果（不转发到下游）
	return emails, nil
}

// collectEmails 收集邮箱
func (h *Handler) collectEmails(ctx context.Context, target string, opts model.EmailTaskOptions) []string {
	var emails []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 并发收集
	collectors := []struct {
		name string
		collect func() ([]string, error)
	}{
		{"github", func() ([]string, error) {
			if !opts.OnlyGitHub && h.githubCollector != nil {
				return h.githubCollector.Collect(ctx, target, opts)
			}
			return nil, nil
		}},
		{"bing", func() ([]string, error) {
			if !opts.OnlyBing && h.bingCollector != nil {
				return h.bingCollector.Collect(ctx, target, opts)
			}
			return nil, nil
		}},
		{"sogou", func() ([]string, error) {
			if !opts.OnlySogou && h.sogouCollector != nil {
				return h.sogouCollector.Collect(ctx, target, opts)
			}
			return nil, nil
		}},
		{"0zone", func() ([]string, error) {
			if !opts.OnlyZerozone && h.zerozoneCollector != nil {
				return h.zerozoneCollector.Collect(ctx, target, opts)
			}
			return nil, nil
		}},
	}

	// 并发执行
	for _, c := range collectors {
		wg.Add(1)
		go func(collector func() ([]string, error)) {
			defer wg.Done()

			// 检查上下文
			select {
			case <-ctx.Done():
				return
			default:
			}

			// 收集邮箱
			result, err := collector()
			if err != nil {
				h.logger.Error("collect failed", "error", err)
				return
			}

			// 添加到结果集
			mu.Lock()
			emails = append(emails, result...)
			mu.Unlock()
		}(c.collect)
	}

	wg.Wait()

	// 去重
	return unique(emails)
}

// unique 去重邮箱列表
func unique(emails []string) []string {
	seen := make(map[string]bool)
	var result []string

	for _, email := range emails {
		if !seen[email] {
			seen[email] = true
			result = append(result, email)
		}
	}

	return result
}