package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// BaseCollector 基础收集器
type BaseCollector struct {
	config *config.Config
	logger *log.Logger
	client *http.Client
}

// NewBaseCollector 创建基础收集器
func NewBaseCollector(cfg *config.Config, logger *log.Logger) *BaseCollector {
	return &BaseCollector{
		config: cfg,
		logger: logger,
		client: &http.Client{
			Timeout: time.Duration(cfg.EmailCollect.RequestTimeout) * time.Second,
		},
	}
}

// GitHubCollector GitHub 邮箱收集器
type GitHubCollector struct {
	*BaseCollector
	repoCache map[string]bool
}

// NewGitHubCollector 创建 GitHub 收集器
func NewGitHubCollector(cfg *config.Config, logger *log.Logger) *GitHubCollector {
	return &GitHubCollector{
		BaseCollector: NewBaseCollector(cfg, logger),
		repoCache:     make(map[string]bool),
	}
}

// Collect 从 GitHub 收集邮箱
func (g *GitHubCollector) Collect(ctx context.Context, target string, opts model.EmailTaskOptions) ([]string, error) {
	var emails []string

	// GitHub 搜索 API
	searchURL := fmt.Sprintf("https://api.github.com/search/code?q=%s+in:file+email&per_page=%d", target, g.config.EmailCollect.GitHub.SearchSize)

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return nil, err
	}

	// 设置认证
	if g.config.EmailCollect.GitHub.Token != "" {
		req.Header.Set("Authorization", "token "+g.config.EmailCollect.GitHub.Token)
	}

	// 发送请求
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API error: %d", resp.StatusCode)
	}

	// 这里简化处理，实际应该解析 JSON 响应
	// 模拟从代码文件中提取邮箱
	patterns := []string{
		`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindAllString(target, -1)

		for _, email := range matches {
			if validateEmail(email) {
				emails = append(emails, email)
			}
		}
	}

	return unique(emails), nil
}

// BingCollector Bing 邮箱收集器
type BingCollector struct {
	*BaseCollector
}

// NewBingCollector 创建 Bing 收集器
func NewBingCollector(cfg *config.Config, logger *log.Logger) *BingCollector {
	return &BingCollector{
		BaseCollector: NewBaseCollector(cfg, logger),
	}
}

// Collect 从 Bing 收集邮箱
func (b *BingCollector) Collect(ctx context.Context, target string, opts model.EmailTaskOptions) ([]string, error) {
	var emails []string

	// Bing 搜索 URL
	searchURL := fmt.Sprintf("https://www.bing.com/search?q=%s+email&count=%d",
		url.QueryEscape(target), b.config.EmailCollect.Bing.SearchSize)

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return nil, err
	}

	// 设置代理（如果有）
	if b.config.EmailCollect.Bing.Proxy != "" {
		proxyURL, err := url.Parse(b.config.EmailCollect.Bing.Proxy)
		if err == nil {
			b.client.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
			}
		}
	}

	// 发送请求
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 解析页面提取邮箱
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}

	doc.Find("a[href*='mailto:'], .b_results a[href*='email']").EachWithBreak(func(i int, s *goquery.Selection) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		email := strings.TrimPrefix(s.Text(), "mailto:")
		email = strings.TrimSpace(email)

		if validateEmail(email) {
			emails = append(emails, email)
		}

		// 限制返回数量
		return len(emails) < b.config.EmailCollect.Bing.SearchSize
	})

	return unique(emails), nil
}

// SogouCollector Sogou 邮箱收集器
type SogouCollector struct {
	*BaseCollector
}

// NewSogouCollector 创建 Sogou 收集器
func NewSogouCollector(cfg *config.Config, logger *log.Logger) *SogouCollector {
	return &SogouCollector{
		BaseCollector: NewBaseCollector(cfg, logger),
	}
}

// Collect 从 Sogou 收集邮箱
func (s *SogouCollector) Collect(ctx context.Context, target string, opts model.EmailTaskOptions) ([]string, error) {
	var emails []string

	// Sogou 搜索 URL
	searchURL := fmt.Sprintf("https://www.sogou.com/web?query=%s+email&page=1&num=%d",
		url.QueryEscape(target), s.config.EmailCollect.Sogou.SearchSize)

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "GET", searchURL, nil)
	if err != nil {
		return nil, err
	}

	// 发送请求
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 解析页面提取邮箱
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}

	doc.Find(".results a[href*='@']").EachWithBreak(func(i int, s *goquery.Selection) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		email := s.Text()
		email = strings.TrimSpace(email)

		if validateEmail(email) {
			emails = append(emails, email)
		}

		// 限制返回数量
		return len(emails) < s.config.EmailCollect.Sogou.SearchSize
	})

	return unique(emails), nil
}

// ZerozoneCollector 0zone 邮箱收集器
type ZerozoneCollector struct {
	*BaseCollector
}

// NewZerozoneCollector 创建 0zone 收集器
func NewZerozoneCollector(cfg *config.Config, logger *log.Logger) *ZerozoneCollector {
	return &ZerozoneCollector{
		BaseCollector: NewBaseCollector(cfg, logger),
	}
}

// Collect 从 0zone 收集邮箱
func (z *ZerozoneCollector) Collect(ctx context.Context, target string, opts model.EmailTaskOptions) ([]string, error) {
	var emails []string

	// 这里简化处理，实际应该调用 0zone API
	// 模拟从 API 获取邮箱

	// 检查 token
	if z.config.EmailCollect.Zerozone.Token == "" {
		return nil, fmt.Errorf("0zone token is required")
	}

	// TODO: 调用 0zone API
	// URL: https://0.zone/api/email?domain=example.com&token=xxx

	// 模拟数据
	emails = append(emails, fmt.Sprintf("admin@%s", target))
	emails = append(emails, fmt.Sprintf("contact@%s", target))

	return unique(emails), nil
}

// RateLimiter 速率限制器
type RateLimiter struct {
	tokens    chan struct{}
	refill    time.Ticker
	maxTokens int
}

// NewRateLimiter 创建速率限制器
func NewRateLimiter(rate int, interval time.Duration) *RateLimiter {
	rl := &RateLimiter{
		tokens:    make(chan struct{}, rate),
		refill:    *time.NewTicker(interval),
		maxTokens: rate,
	}

	// 初始化令牌
	for i := 0; i < rate; i++ {
		rl.tokens <- struct{}{}
	}

	// 定期补充令牌
	go func() {
		for range rl.refill.C {
			select {
			case rl.tokens <- struct{}{}:
			default:
				// 通道已满
			}
		}
	}()

	return rl
}

// Acquire 获取令牌
func (rl *RateLimiter) Acquire() <-chan struct{} {
	return rl.tokens
}

// Close 关闭限制器
func (rl *RateLimiter) Close() {
	rl.refill.Stop()
	close(rl.tokens)
}

// validateEmail 验证邮箱格式
func validateEmail(email string) bool {
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(email)
}