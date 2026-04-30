package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	taskConfig "github.com/Populurs/taskcore/config"
	taskLog "github.com/Populurs/taskcore/log"
	taskRepo "github.com/Populurs/taskcore/repository"
	taskController "github.com/Populurs/taskcore/controller"
	emailConfig "email-collect/internal/config"
	emailHandler "email-collect/internal/handler"
	emailRepo "email-collect/internal/repository"
)

func main() {
	// 保留旧 CLI 支持
	if len(os.Args) > 1 && os.Args[1] == "--legacy-cli" {
		main1()
		return
	}

	// 加载配置
	conf, err := emailConfig.LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 初始化日志
	logger := taskLog.NewLog(&conf.BaseConfig.Log)

	// 初始化数据库
	webDB := taskRepo.NewDB(&conf.BaseConfig.Data.DB.Web)
	asmDB := taskRepo.NewDB(&conf.BaseConfig.Data.DB.Asm)
	repoBase := taskRepo.NewRepository(logger, webDB, asmDB)

	// 创建邮箱仓库
	emailRepo := emailRepo.NewEmailRepository(repoBase)

	// 创建处理器
	h := emailHandler.NewHandler(logger, emailRepo, conf)

	// 初始化任务分发器
	dealer, err := taskController.InitDealer(&conf.BaseConfig, logger, h, taskController.DealerConfig{
		ModuleName:         "email.collect",
		DefaultOSSBasePath: "stream/email",
	})
	if err != nil {
		logger.Error("Failed to init dealer", "error", err)
		os.Exit(1)
	}

	// 订阅事件
	err = dealer.SubscribeEvent()
	if err != nil {
		logger.Error("Failed to subscribe event", "error", err)
		os.Exit(1)
	}

	logger.Info("Email-Collect service started")

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 优雅关闭
	logger.Info("Shutting down...")
	dealer.Close()
	logger.Info("Email-Collect service stopped")
}

// main1 保留旧 CLI 的实现
func main1() {
	fmt.Println("Legacy CLI mode - this will be removed in future versions")
}