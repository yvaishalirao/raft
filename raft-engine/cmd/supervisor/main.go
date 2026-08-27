package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: supervisor <kill|restart> <container>")
		os.Exit(1)
	}
	action, container := os.Args[1], os.Args[2]
	composeFile := envOr("COMPOSE_FILE", "docker-compose.yml")

	var cmd *exec.Cmd
	switch action {
	case "kill":
		// A demo kill must be an unclean SIGKILL, never a graceful
		// SIGTERM (docker stop) — a real crash, not a shutdown.
		cmd = exec.Command("docker", "kill", "--signal=SIGKILL", container)
	case "restart":
		cmd = exec.Command("docker", "compose", "-f", composeFile, "start", container)
	default:
		fmt.Fprintf(os.Stderr, "unknown action %q\n", action)
		os.Exit(1)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "supervisor: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
