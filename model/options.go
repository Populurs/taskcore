package model

import (
	"encoding/json"
	"fmt"
)

func ResolveOptions[T any](p TaskPayload, moduleName string, apply func(*T, json.RawMessage) error) (T, error) {
	var options T
	raw := BytesTrimSpace(p.Options)
	if len(raw) == 0 || string(raw) == "null" {
		return options, nil
	}

	root := make(map[string]json.RawMessage)
	if err := json.Unmarshal(raw, &root); err != nil {
		return options, fmt.Errorf("parse payload.options for module %q failed: %w", moduleName, err)
	}

	isNewFormat := HasKey(root, "common") || HasKey(root, moduleName) || HasKey(root, "modules")
	if isNewFormat {
		if commonRaw, ok := root["common"]; ok {
			if err := apply(&options, commonRaw); err != nil {
				return options, err
			}
		}
		if moduleRaw, ok := root[moduleName]; ok {
			if err := apply(&options, moduleRaw); err != nil {
				return options, err
			}
		}
		if modulesRaw, ok := root["modules"]; ok {
			modules := make(map[string]json.RawMessage)
			if err := json.Unmarshal(modulesRaw, &modules); err == nil {
				if moduleRaw, ok := modules[moduleName]; ok {
					if err = apply(&options, moduleRaw); err != nil {
						return options, err
					}
				}
			}
		}
		return options, nil
	}

	// legacy format
	if err := json.Unmarshal(raw, &options); err != nil {
		return options, fmt.Errorf("parse legacy options failed: %w", err)
	}
	return options, nil
}
