package testutils

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// GetTopicPartitionCount parses the project topics yaml file to find the number of partitions for a topic.
func GetTopicPartitionCount(topicName string) int {
	yamlPath := filepath.Join(GetProjectRoot(nil), "infra", "kafka", "topics.yaml")
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return 12 // safe fallback
	}
	blocks := strings.Split(string(data), "---")
	for _, block := range blocks {
		if strings.Contains(block, "name: "+topicName) {
			re := regexp.MustCompile(`partitions:\s*(\d+)`)
			matches := re.FindStringSubmatch(block)
			if len(matches) > 1 {
				if p, err := strconv.Atoi(matches[1]); err == nil {
					return p
				}
			}
		}
	}
	return 12
}
