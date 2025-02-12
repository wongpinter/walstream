package cleanup

import (
	"fmt"
	"strings"
)

// resultCollector implements ResultCollector interface
type resultCollector struct {
	results []CleanupResult
}

// NewResultCollector creates a new result collector
func NewResultCollector() ResultCollector {
	return &resultCollector{
		results: make([]CleanupResult, 0),
	}
}

// AddResult implements ResultCollector
func (rc *resultCollector) AddResult(result CleanupResult) {
	rc.results = append(rc.results, result)
}

// GetResults implements ResultCollector
func (rc *resultCollector) GetResults() []CleanupResult {
	return rc.results
}

// HasErrors implements ResultCollector
func (rc *resultCollector) HasErrors() bool {
	for _, result := range rc.results {
		if !result.Success && !result.Skipped {
			return true
		}
	}
	return false
}

// GetSummary implements ResultCollector
func (rc *resultCollector) GetSummary() string {
	var summary strings.Builder
	var successful, skipped, failed int

	// Count results by type
	for _, result := range rc.results {
		if result.Skipped {
			skipped++
		} else if result.Success {
			successful++
		} else {
			failed++
		}
	}

	summary.WriteString("\nCleanup Operation Summary\n")
	summary.WriteString("=======================\n\n")

	// Overall statistics
	summary.WriteString(fmt.Sprintf("Total operations    : %d\n", len(rc.results)))
	summary.WriteString(fmt.Sprintf("Successful          : %d\n", successful))
	summary.WriteString(fmt.Sprintf("Skipped            : %d\n", skipped))
	summary.WriteString(fmt.Sprintf("Failed             : %d\n\n", failed))

	// Details section
	if failed > 0 {
		summary.WriteString("Failed Operations:\n")
		summary.WriteString("-----------------\n")
		for _, result := range rc.results {
			if !result.Success && !result.Skipped {
				summary.WriteString(fmt.Sprintf("- [%s] %s: %v\n",
					result.Resource.Type,
					result.Resource.Name,
					result.Error))
			}
		}
		summary.WriteString("\n")
	}

	if skipped > 0 {
		summary.WriteString("Skipped Operations:\n")
		summary.WriteString("------------------\n")
		for _, result := range rc.results {
			if result.Skipped {
				summary.WriteString(fmt.Sprintf("- [%s] %s: %s\n",
					result.Resource.Type,
					result.Resource.Name,
					result.Message))
			}
		}
		summary.WriteString("\n")
	}

	// Add recommendations based on results
	if failed > 0 {
		summary.WriteString("Recommendations:\n")
		summary.WriteString("---------------\n")
		summary.WriteString("1. Check permissions for failed operations\n")
		summary.WriteString("2. Verify resource dependencies are correct\n")
		summary.WriteString("3. Consider using --force flag for locked resources\n")
		summary.WriteString("4. Review error messages for specific actions needed\n\n")
	}

	return summary.String()
}
