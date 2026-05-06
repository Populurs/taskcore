package config

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type Configurable interface {
	GetBase() *BaseConfig
}

func LoadConfig[T Configurable](configPath string, newConf func() T, applyDefaults func(T)) T {
	conf := newConf()

	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Errorf("fatal error read config file: %w", err))
	}
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("config file changed:", e.Name)
		if err := v.Unmarshal(conf); err != nil {
			fmt.Println(err)
		}
	})
	if err := v.Unmarshal(conf); err != nil {
		panic(fmt.Errorf("fatal error unmarshal config: %w", err))
	}

	base := conf.GetBase()
	base.ApplyBaseDefaults()
	applyDefaults(conf)

	return conf
}
