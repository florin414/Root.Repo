package sdk

import (
	"fmt"
	"github.com/google/uuid"
	"os"
	"time"
)

func generateID() string {
	id := uuid.New()
	return id.String()
}

func getServiceName() string {
	if serviceName := os.Getenv("SERVICE_NAME"); serviceName != "" {
		return serviceName
	}

	if exe, err := os.Executable(); err == nil {
		return exe
	}

	return fmt.Sprintf("service-%d", time.Now().UnixNano())
}

func getHostname() string {
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}

	if hostname := os.Getenv("HOSTNAME"); hostname != "" {
		return hostname
	}

	return fmt.Sprintf("host-%d", time.Now().UnixNano())
}
