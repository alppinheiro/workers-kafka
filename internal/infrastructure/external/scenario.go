package external

import (
	"fmt"
	"strings"
)

type scenarioMode string

const (
	scenarioNone      scenarioMode = ""
	scenarioFail      scenarioMode = "fail"
	scenarioRetryOnce scenarioMode = "retry-once"
	scenarioRetry     scenarioMode = "retry"
)

func matchScenario(orderID string, stage string) scenarioMode {
	switch {
	case strings.Contains(orderID, stage+"-fail"):
		return scenarioFail
	case strings.Contains(orderID, stage+"-retry-once"):
		return scenarioRetryOnce
	case strings.Contains(orderID, stage+"-retry"):
		return scenarioRetry
	default:
		return scenarioNone
	}
}

func retryError(stage string, attempt int) error {
	return fmt.Errorf("debug scenario for %s: temporary failure on attempt %d", stage, attempt)
}
