package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	yaml "gopkg.in/yaml.v2"
)

// DBStruct defines fields for databases
type DBStruct struct {
	User   string `yaml:"user"`
	Passwd string `yaml:"passwd"`
	Host   string `yaml:"host"`
	Port   string `yaml:"port"`
	Name   string `yaml:"name"`
}

// ServerStruct defines fields for main logic
type ServerStruct struct {
	Brokers   []string `yaml:"brokers"`
	Topics    []string `yaml:"topics"`
	Group     string   `yaml:"group"`
	AdminAddr string   `yaml:"admin_addr"`

	ConsumeRoutines   int           `yaml:"consume_routines"`
	ConsumeBuffer     int           `yaml:"consume_buffer"`
	EngineRoutines    int           `yaml:"engine_routines"`
	EngineBatch       int           `yaml:"engine_batch"`
	EngineBuffer      int           `yaml:"engine_buffer"`
	EngineDelayTime   time.Duration `yaml:"engine_delay_time"`
	EngineRefreshTime time.Duration `yaml:"engine_refresh_time"`
	StorageRoutines   int           `yaml:"storage_routines"`
	StorageDelayTime  time.Duration `yaml:"storage_delay_time"`
	StorageWaitTime   time.Duration `yaml:"storage_wait_time"`
}

// RedisStruct defines fields for redis
type RedisStruct struct {
	Passwd string `yaml:"passwd"`
	Addr   string `yaml:"addr"`
	DB     int    `yaml:"db"`
}

// Config structure for server
type Config struct {
	Server ServerStruct `yaml:"server"`
	Redis  RedisStruct  `yaml:"redis"`
	DB     DBStruct     `yaml:"db"`
}

// ParseYamlFile the config file
func ParseYamlFile(filename string, c *Config) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, c)
}

// Check validates the fields in config file
func (c *Config) Check() error {
	if len(c.Server.Topics) == 0 {
		return errors.New("invalid topics")
	}

	if len(c.Server.Group) == 0 {
		return errors.New("invalid group")
	}

	if c.Server.StorageDelayTime < c.Server.EngineDelayTime*2 {
		return fmt.Errorf("storage delay time(%ds) should be double and more than engine(%ds)", c.Server.StorageDelayTime, c.Server.EngineDelayTime)
	}

	return nil
}
