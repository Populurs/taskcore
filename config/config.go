package config

import "time"

const (
	WebDB = "web"
	AsmDB = "asm"
)

type BaseConfig struct {
	Rabbitmq                 Rabbitmq   `mapstructure:"rabbitmq" json:"rabbitmq" yaml:"rabbitmq"`
	Log                      LogConfig  `mapstructure:"log" json:"log" yaml:"log"`
	Data                     DataConfig `mapstructure:"data" json:"data" yaml:"data"`
	AliyunOSS                AliyunOSS  `mapstructure:"aliyun-oss" json:"aliyun-oss" yaml:"aliyun-oss"`
	TopicJobStart            string     `mapstructure:"topic_job_start" json:"topic_job_start" yaml:"topic_job_start"`
	TopicDependencyCompleted []string   `mapstructure:"topic_dep_completed" json:"topic_dep_completed" yaml:"topic_dep_completed"`
	TopicWhoRelayOn          []string   `mapstructure:"topic_who_relay_on" json:"topic_who_relay_on" yaml:"topic_who_relay_on"`
	ModulesRelayOn           []string   `mapstructure:"modules_relay_on" json:"modules_relay_on" yaml:"modules_relay_on"`
	TopicJobStop             string     `mapstructure:"topic_job_stop" json:"topic_job_stop" yaml:"topic_job_stop"`
	Concurrent               int        `mapstructure:"concurrent" json:"concurrent" yaml:"concurrent"`
	RelayShardMaxItems       int        `mapstructure:"relay_shard_max_items" json:"relay_shard_max_items" yaml:"relay_shard_max_items"`
	TopicResultFeedback      []string   `mapstructure:"topic_result_feedback" json:"topic_result_feedback" yaml:"topic_result_feedback"`
	EnableResultFeedback     bool       `mapstructure:"enable_result_feedback" json:"enable_result_feedback" yaml:"enable_result_feedback"`
}

func (c *BaseConfig) ApplyBaseDefaults() {
	if c.AliyunOSS.Region == "" {
		c.AliyunOSS.Region = "cn-hangzhou"
	}
	if c.Concurrent <= 0 {
		c.Concurrent = 5
	}
	if c.RelayShardMaxItems <= 0 {
		c.RelayShardMaxItems = 100
	}
}

type Rabbitmq struct {
	Url           string `yaml:"url" mapstructure:"url"`
	Vhost         string `yaml:"vhost" mapstructure:"vhost"`
	PrefetchCount int    `yaml:"prefetch_count" mapstructure:"prefetch_count"`
}

type LogConfig struct {
	LogLevel    string `yaml:"log_level" mapstructure:"log_level"`
	Mode        string `yaml:"mode" mapstructure:"mode"`
	Encoding    string `yaml:"encoding" mapstructure:"encoding"`
	LogFileName string `yaml:"log_file_name" mapstructure:"log_file_name"`
	MaxBackups  int    `yaml:"max_backups" mapstructure:"max_backups"`
	MaxAge      int    `yaml:"max_age" mapstructure:"max_age"`
	MaxSize     int    `yaml:"max_size" mapstructure:"max_size"`
	Compress    bool   `yaml:"compress" mapstructure:"compress"`
}

type RedisConfig struct {
	Addr         string        `mapstructure:"addr" yaml:"addr"`
	Password     string        `mapstructure:"password" yaml:"password"`
	DB           int           `mapstructure:"db" yaml:"db"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout" yaml:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout" yaml:"write_timeout"`
	JobTTL       time.Duration `mapstructure:"job_ttl" yaml:"job_ttl"`
}

type DataConfig struct {
	DB    DBConfig    `yaml:"db" mapstructure:"db"`
	Redis RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type DBConfig struct {
	Web DBConnection `yaml:"web" mapstructure:"web"`
	Asm DBConnection `yaml:"asm" mapstructure:"asm"`
}

type DBConnection struct {
	Driver       string `yaml:"driver" mapstructure:"driver"`
	DSN          string `yaml:"dsn" mapstructure:"dsn"`
	MaxIdleConns int    `yaml:"max_idle_conns" mapstructure:"max_idle_conns"`
	MaxOpenConns int    `yaml:"max_open_conns" mapstructure:"max_open_conns"`
	MaxLifeTime  int    `yaml:"conn_max_lifetime" mapstructure:"conn_max_lifetime"`
}

type AliyunOSS struct {
	Endpoint            string `mapstructure:"endpoint" json:"endpoint" yaml:"endpoint"`
	Region              string `mapstructure:"region" json:"region" yaml:"region"`
	AccessKeyID         string `mapstructure:"access-key-id" json:"access-key-id" yaml:"access-key-id"`
	AccessKeySecret     string `mapstructure:"access-key-secret" json:"access-key-secret" yaml:"access-key-secret"`
	BucketName          string `mapstructure:"bucket-name" json:"bucket-name" yaml:"bucket-name"`
	BucketURL           string `mapstructure:"bucket-url" json:"bucket-url" yaml:"bucket-url"`
	BasePath            string `mapstructure:"base-path" json:"base-path" yaml:"base-path"`
	DisableSSL          bool   `mapstructure:"disable-ssl" json:"disable-ssl" yaml:"disable-ssl"`
	UseInternalEndpoint bool   `mapstructure:"use-internal-endpoint" json:"use-internal-endpoint" yaml:"use-internal-endpoint"`
}
