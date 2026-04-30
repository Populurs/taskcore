package config

import (
	"time"

	"github.com/Populurs/taskcore/config"
	"github.com/spf13/viper"
)

type Config struct {
	config.BaseConfig `mapstructure:",squash"`
	EmailCollect      EmailCollectConfig `mapstructure:"email_collect"`
}

// EmailCollectConfig 邮箱收集模块配置
type EmailCollectConfig struct {
	// 数据源配置
	GitHub GitHubConfig `mapstructure:"github"`
	Bing   BingConfig   `mapstructure:"bing"`
	Sogou  SogouConfig  `mapstructure:"sogou"`
	Zerozone ZerozoneConfig `mapstructure:"0zone"`

	// 收集配置
	MaxRetryCount int `mapstructure:"max_retry_count"`
	RequestTimeout int `mapstructure:"request_timeout"`
}

// GitHubConfig GitHub 数据源配置
type GitHubConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	Token      string `mapstructure:"token"`
	SearchSize int    `mapstructure:"search_size"`
}

// BingConfig Bing 数据源配置
type BingConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	Proxy      string `mapstructure:"proxy"`
	SearchSize int    `mapstructure:"search_size"`
}

// SogouConfig Sogou 数据源配置
type SogouConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	SearchSize int    `mapstructure:"search_size"`
}

// ZerozoneConfig 0zone 数据源配置
type ZerozoneConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Token   string `mapstructure:"token"`
}

// Configurable 实现 TaskCore 的配置接口
func (c *Config) GetBase() *config.BaseConfig {
	return &c.BaseConfig
}

// LoadConfig 加载配置
func LoadConfig() (*Config, error) {
	viper.SetConfigName("email_conf")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	// 设置默认值
	viper.SetDefault("email_collect.max_retry_count", 3)
	viper.SetDefault("email_collect.request_timeout", 10)
	viper.SetDefault("email_collect.github.search_size", 50)
	viper.SetDefault("email_collect.bing.search_size", 50)
	viper.SetDefault("email_collect.sogou.search_size", 50)

	var config Config
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}