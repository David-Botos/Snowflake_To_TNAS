// pkg/converter/array.go
package converter

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// convertToJSON handles conversion of complex types to JSONB
func (c *TypeConverter) convertToJSON(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return nil, nil
		}
		
		// Check if already valid JSON
		var js interface{}
		if err := json.Unmarshal([]byte(v), &js); err == nil {
			return v, nil
		}
		
		// If not valid JSON, wrap string in quotes
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(v, "\"", "\\\"")), nil
		
	case []byte:
		if len(v) == 0 {
			return nil, nil
		}
		
		// Check if already valid JSON
		var js interface{}
		if err := json.Unmarshal(v, &js); err == nil {
			return string(v), nil
		}
		
		// Try to convert binary to string then to JSON
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(string(v), "\"", "\\\"")), nil
		
	case nil:
		return nil, nil
		
	default:
		// For array and object types from Snowflake
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
		}
		return string(jsonBytes), nil
	}
}

// HandleSnowflakeArray processes Snowflake ARRAY types
func (c *TypeConverter) HandleSnowflakeArray(value interface{}) ([]byte, error) {
	// Handle different incoming types
	switch v := value.(type) {
	case string:
		// If looks like JSON array, use as is
		if strings.HasPrefix(strings.TrimSpace(v), "[") && strings.HasSuffix(strings.TrimSpace(v), "]") {
			return []byte(v), nil
		}
		// Otherwise, treat as single element array
		return json.Marshal([]string{v})
		
	case []interface{}:
		return json.Marshal(v)
		
	case nil:
		return []byte("[]"), nil
		
	default:
		// Check if value is a slice using reflection
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Slice {
			// Convert slice to []interface{}
			length := val.Len()
			result := make([]interface{}, length)
			for i := 0; i < length; i++ {
				result[i] = val.Index(i).Interface()
			}
			return json.Marshal(result)
		}
		
		// Single value, convert to single-element array
		return json.Marshal([]interface{}{value})
	}
}

// HandleSnowflakeObject processes Snowflake OBJECT types
func (c *TypeConverter) HandleSnowflakeObject(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		return json.Marshal(v)
		
	case string:
		// If it looks like JSON object
		if strings.HasPrefix(strings.TrimSpace(v), "{") && strings.HasSuffix(strings.TrimSpace(v), "}") {
			return []byte(v), nil
		}
		
		// Try to parse as JSON
		var jsonMap map[string]interface{}
		if err := json.Unmarshal([]byte(v), &jsonMap); err == nil {
			return []byte(v), nil
		}
		
		// Fallback: create object with default field
		return json.Marshal(map[string]interface{}{"value": v})
		
	case nil:
		return []byte("{}"), nil
		
	default:
		// Attempt to convert to map using reflection
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Map {
			result := make(map[string]interface{})
			for _, key := range val.MapKeys() {
				result[fmt.Sprintf("%v", key.Interface())] = val.MapIndex(key).Interface()
			}
			return json.Marshal(result)
		}
		
		// Struct conversion
		if val.Kind() == reflect.Struct {
			jsonBytes, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			return jsonBytes, nil
		}
		
		// Fallback: create object with default field
		return json.Marshal(map[string]interface{}{"value": value})
	}
}