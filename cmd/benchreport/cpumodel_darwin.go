package main

import "golang.org/x/sys/unix"

// platformCPUModel reads the CPU brand string from the kernel directly.
func platformCPUModel() string {
	model, err := unix.Sysctl("machdep.cpu.brand_string")
	if err != nil {
		return ""
	}
	return model
}
