module email-collect

go 1.26.1

replace github.com/Populurs/taskcore => ../../..

require (
	github.com/aliyun/alibabacloud-oss-go-sdk-v2 v1.4.0
	github.com/PuerkitoBio/goquery v1.8.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/viper v1.21.0
	go.uber.org/zap v1.27.1
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gorm.io/driver/mysql v1.6.0
	gorm.io/driver/postgres v1.6.0
	gorm.io/gorm v1.31.1
)