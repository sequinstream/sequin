package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
)

func MergeJSONConfig(config interface{}, jsonInput string, jsonFile string) error {
	var jsonData []byte
	var err error

	if jsonInput != "" {
		jsonData = []byte(jsonInput)
	} else if jsonFile != "" {
		jsonData, err = ioutil.ReadFile(jsonFile)
		if err != nil {
			return fmt.Errorf("failed to read JSON file: %w", err)
		}
	} else {
		return nil // No JSON input provided, nothing to merge
	}

	// Create a map to hold the JSON data
	var jsonMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &jsonMap); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Use reflection to set the values in the config struct
	configValue := reflect.ValueOf(config).Elem()
	configType := configValue.Type()

	for i := 0; i < configValue.NumField(); i++ {
		field := configType.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			jsonTag = field.Name
		}

		if value, ok := jsonMap[jsonTag]; ok {
			fieldValue := configValue.Field(i)
			if fieldValue.CanSet() {
				newValue := reflect.ValueOf(value)
				if newValue.Type().ConvertibleTo(fieldValue.Type()) {
					fieldValue.Set(newValue.Convert(fieldValue.Type()))
				}
			}
		}
	}

	return nil
}
