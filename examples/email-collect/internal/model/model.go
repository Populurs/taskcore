package model

import (
	"time"
)

// EmailPersistPayload 邮箱持久化数据
type EmailPersistPayload struct {
	Email      string    `json:"email" gorm:"primaryKey"`
	Source     string    `json:"source"`      // 来源：github/bing/sogou/0zone
	Target     string    `json:"target"`      // 原始目标（域名）
	TaskID     string    `json:"task_id"`     // 任务ID
	WorkTaskID string    `json:"work_task_id"`// 工作任务ID
	TenantID   string    `json:"tenant_id"`   // 租户ID
	UserID     string    `json:"user_id"`     // 用户ID
	CreateTime time.Time `json:"create_time"` // 创建时间
	UpdateTime time.Time `json:"update_time"` // 更新时间
}

// EmailTaskOptions 邮箱收集任务选项
type EmailTaskOptions struct {
	// 数据源配置
	OnlyGitHub bool `json:"only_github"`
	OnlyBing   bool `json:"only_bing"`
	OnlySogou  bool `json:"only_sogou"`
	OnlyZerozone bool `json:"only_0zone"`

	// 收集配置
	MinConfidence float64 `json:"min_confidence"` // 最小置信度 (0.0-1.0)
	MaxEmails     int     `json:"max_emails"`     // 最大收集数量 (0表示不限制)
}

// ApplyEmailOptions 应用默认邮箱选项
func ApplyEmailOptions(base map[string]any, module map[string]any) EmailTaskOptions {
	options := EmailTaskOptions{
		MinConfidence: 0.8,
		MaxEmails:     0,
	}

	// 合并基础配置
	if base != nil {
		if v, ok := base["only_github"]; ok {
			options.OnlyGitHub = v.(bool)
		}
		if v, ok := base["only_bing"]; ok {
			options.OnlyBing = v.(bool)
		}
		if v, ok := base["only_sogou"]; ok {
			options.OnlySogou = v.(bool)
		}
		if v, ok := base["only_0zone"]; ok {
			options.OnlyZerozone = v.(bool)
		}
		if v, ok := base["min_confidence"]; ok {
			options.MinConfidence = v.(float64)
		}
		if v, ok := base["max_emails"]; ok {
			options.MaxEmails = v.(int)
		}
	}

	// 合并模块特定配置
	if module != nil {
		if v, ok := module["only_github"]; ok {
			options.OnlyGitHub = v.(bool)
		}
		if v, ok := module["only_bing"]; ok {
			options.OnlyBing = v.(bool)
		}
		if v, ok := module["only_sogou"]; ok {
			options.OnlySogou = v.(bool)
		}
		if v, ok := module["only_0zone"]; ok {
			options.OnlyZerozone = v.(bool)
		}
		if v, ok := module["min_confidence"]; ok {
			options.MinConfidence = v.(float64)
		}
		if v, ok := module["max_emails"]; ok {
			options.MaxEmails = v.(int)
		}
	}

	return options
}