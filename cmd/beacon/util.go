package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strings"
)

// runShell executes a command through the system shell and returns output.
func runShell(shell, command string) (string, int, error) {
	var stdout, stderr bytes.Buffer

	cmd := exec.Command(shell)
	cmd.Stdin = strings.NewReader(command)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		exitCode := 1
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
		output := stdout.String()
		if errMsg := stderr.String(); errMsg != "" {
			if output != "" {
				output += "\n"
			}
			output += errMsg
		}
		return output, exitCode, err
	}

	output := stdout.String()
	if errMsg := stderr.String(); errMsg != "" {
		if output != "" {
			output += "\n"
		}
		output += errMsg
	}

	return output, cmd.ProcessState.ExitCode(), nil
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func getCWD() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "unknown"
	}
	return cwd
}

func getCurrentUser() string {
	u, err := user.Current()
	if err != nil {
		return "unknown"
	}

	info := fmt.Sprintf("Username: %s\nUID: %s\nGID: %s\nHome: %s",
		u.Username, u.Uid, u.Gid, u.HomeDir)

	// Check for elevated privileges
	if runtime.GOOS != "windows" {
		if os.Geteuid() == 0 {
			info += "\nPrivilege: root"
		} else {
			info += "\nPrivilege: user"
		}
	}

	return info
}
