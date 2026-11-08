package orchestration

import (
	"encoding/json"
	"reflect"
)

func jsonValuesEqual(left, right any) bool {
	return reflect.DeepEqual(left, right)
}

func cloneJSON[T any](value T) (T, error) {
	var clone T
	data, err := json.Marshal(value)
	if err != nil {
		return clone, err
	}
	if err := json.Unmarshal(data, &clone); err != nil {
		return clone, err
	}
	return clone, nil
}
