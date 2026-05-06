package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type EventMetadata struct {
	EventType   string `json:"event_type"`
	TenantID    uint32 `json:"tenant_id"`
	UserID      uint32 `json:"user_id"`
	TaskID      uint32 `json:"task_id"`
	WorkTaskID  uint32 `json:"work_task_id"`
	CompanyID   uint32 `json:"company_id"`
	CompanyName string `json:"company_name"`
}

type TaskPayload struct {
	OssPath        string          `json:"oss_path"`
	Options        json.RawMessage `json:"options,omitempty"`
	EnabledModules []string        `json:"enabled_modules,omitempty"`
	PayloadVersion string          `json:"payload_version,omitempty"`
}

func (p TaskPayload) IsModuleEnabled(module string) bool {
	module = strings.TrimSpace(strings.ToLower(module))
	if module == "" {
		return true
	}
	if len(p.EnabledModules) == 0 {
		return true
	}
	for _, item := range p.EnabledModules {
		if strings.TrimSpace(strings.ToLower(item)) == module {
			return true
		}
	}
	return false
}

func HasKey(m map[string]json.RawMessage, key string) bool {
	_, ok := m[key]
	return ok
}

func BytesTrimSpace(v []byte) []byte {
	return []byte(strings.TrimSpace(string(v)))
}

func MapToEventMetadata(m map[string]string) (*EventMetadata, error) {
	em := &EventMetadata{}
	em.EventType = m["event_type"]
	em.CompanyName = m["company_name"]

	if val, ok := m["tenant_id"]; ok && val != "" {
		u, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid tenant_id: %w", err)
		}
		em.TenantID = uint32(u)
	}
	if val, ok := m["user_id"]; ok && val != "" {
		u, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid user_id: %w", err)
		}
		em.UserID = uint32(u)
	}
	if val, ok := m["task_id"]; ok && val != "" {
		u, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid task_id: %w", err)
		}
		em.TaskID = uint32(u)
	}
	if val, ok := m["work_task_id"]; ok && val != "" {
		u, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid work_task_id: %w", err)
		}
		em.WorkTaskID = uint32(u)
	}
	if val, ok := m["company_id"]; ok && val != "" {
		u, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid company_id: %w", err)
		}
		em.CompanyID = uint32(u)
	}

	return em, nil
}

func TaskKey(metadata *EventMetadata) string {
	return fmt.Sprintf("%d-%d", metadata.TaskID, metadata.WorkTaskID)
}

func EventMetadataToMap(em *EventMetadata) map[string]string {
	if em == nil {
		return map[string]string{}
	}
	return map[string]string{
		"event_type":   em.EventType,
		"tenant_id":    strconv.FormatUint(uint64(em.TenantID), 10),
		"user_id":      strconv.FormatUint(uint64(em.UserID), 10),
		"task_id":      strconv.FormatUint(uint64(em.TaskID), 10),
		"work_task_id": strconv.FormatUint(uint64(em.WorkTaskID), 10),
		"company_id":   strconv.FormatUint(uint64(em.CompanyID), 10),
		"company_name": em.CompanyName,
	}
}

func ResultKeyFromMetadata(metadata *EventMetadata) string {
	return fmt.Sprintf("%d-%d", metadata.TaskID, metadata.WorkTaskID)
}

func JoinJSONStrings(jsonStrs []string) ([]byte, error) {
	if len(jsonStrs) == 0 {
		return []byte("[]"), nil
	}

	var builder strings.Builder
	builder.WriteRune('[')

	// 连接所有JSON字符串，用逗号分隔
	for i, jsonStr := range jsonStrs {
		if i > 0 {
			builder.WriteRune(',')
		}
		builder.WriteString(jsonStr)
	}

	builder.WriteRune(']')

	// 将结果转换为[]byte
	result := []byte(builder.String())

	// 验证生成的JSON是否有效
	/*
		var temp interface{}
		if err := json.Unmarshal(result, &temp); err != nil {
			return nil, fmt.Errorf("生成的JSON无效: %w", err)
		}
	*/

	return result, nil
}
