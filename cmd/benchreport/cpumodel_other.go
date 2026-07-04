//go:build !darwin && !linux

package main

// platformCPUModel has no portable source on this platform.
func platformCPUModel() string { return "" }
