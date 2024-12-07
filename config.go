package main

import (
	"errors"
	"fmt"
	"net/url"
)

type TestType string

const (
	TypeLatency    TestType = "latency"
	TypeThroughput TestType = "throughput"
)

type Config struct {
	GeyserUrl   string   `json:"geyser_url"`
	GeyserToken string   `json:"geyser_token"`
	Type        TestType `json:"bench_type"`
	Duration    int64    `json:"duration_sec"`
}

func (c *Config) Validate() error {
	// parse the url
	u, err := url.Parse(c.GeyserUrl)
	if err != nil {
		return fmt.Errorf("invalid gRPC address provided: %v", err)
	}

	// verify the hostname
	hostname := u.Hostname()
	if hostname == "" {
		return errors.New("invalid gRPC address provided, please provide URL format endpoint e.g. http(s)://<endpoint>:<port>")
	}

	if c.Duration < 10 {
		return errors.New("invalid duration value provided, duration must be greater than 10 seconds")
	}

	if c.Type != TypeLatency && c.Type != TypeThroughput {
		return errors.New("invalid bench type, must be 'latency' or 'throughput'")
	}

	return nil
}
