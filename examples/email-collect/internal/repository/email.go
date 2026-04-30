package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Populurs/taskcore/model"
	"github.com/Populurs/taskcore/repository"
	taskRepo "github.com/Populurs/taskcore/repository"
	"gorm.io/gorm"
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

// EmailRepository 邮箱数据仓库
type EmailRepository struct {
	*repository.Repository
}

// NewEmailRepository 创建邮箱数据仓库
func NewEmailRepository(repoBase *taskRepo.Repository) *EmailRepository {
	return &EmailRepository{Repository: repoBase}
}

// SaveResults 批量保存邮箱结果
func (r *EmailRepository) SaveResults(ctx context.Context, metadata *model.EventMetadata, emailsChan chan []EmailPersistPayload) error {
	return r.Transaction(ctx, func(ctx context.Context) error {
		var batch []EmailPersistPayload

		// 从通道接收数据
		select {
		case batch = <-emailsChan:
			// 继续处理
		case <-ctx.Done():
			return ctx.Err()
		}

		if len(batch) == 0 {
			return nil
		}

		// 批量插入
		now := time.Now()
		for i := range batch {
			batch[i].TenantID = fmt.Sprintf("%d", metadata.TenantID)
			batch[i].UserID = fmt.Sprintf("%d", metadata.UserID)
			batch[i].TaskID = fmt.Sprintf("%d", metadata.TaskID)
			batch[i].WorkTaskID = fmt.Sprintf("%d", metadata.WorkTaskID)
			batch[i].UpdateTime = now
			if batch[i].CreateTime.IsZero() {
				batch[i].CreateTime = now
			}
		}

		result := r.DB(ctx).CreateInBatches(batch, 1000)
		if result.Error != nil {
			return fmt.Errorf("failed to save emails: %w", result.Error)
		}

		return nil
	})
}

// GetEmailsByTask 获取任务中的邮箱列表
func (r *EmailRepository) GetEmailsByTask(ctx context.Context, taskID string) ([]EmailPersistPayload, error) {
	var emails []EmailPersistPayload
	result := r.DB(ctx).Where("task_id = ?", taskID).Find(&emails)
	if result.Error != nil {
		return nil, result.Error
	}
	return emails, nil
}

// EmailCount 统计任务中的邮箱数量
func (r *EmailRepository) EmailCount(ctx context.Context, taskID string) (int64, error) {
	var count int64
	result := r.DB(ctx).Model(&EmailPersistPayload{}).Where("task_id = ?", taskID).Count(&count)
	if result.Error != nil {
		return 0, result.Error
	}
	return count, nil
}

// DeleteTaskEmails 删除任务的所有邮箱
func (r *EmailRepository) DeleteTaskEmails(ctx context.Context, taskID string) error {
	result := r.DB(ctx).Where("task_id = ?", taskID).Delete(&EmailPersistPayload{})
	if result.Error != nil {
		return result.Error
	}
	return nil
}