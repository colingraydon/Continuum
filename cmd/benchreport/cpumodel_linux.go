package main

import (
	"os"
	"strings"
)

// platformCPUModel extracts the model name from /proc/cpuinfo.
func platformCPUModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "model name"); ok {
			return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(rest), ":"))
		}
	}
	return ""
}
