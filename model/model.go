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