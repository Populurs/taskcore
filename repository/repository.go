package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Populurs/taskcore/config"
	"github.com/Populurs/taskcore/log"
	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const ctxTxKey = "TxKey"
const CtxDbKey = "DbKey"

type Repository struct {
	webDb  *gorm.DB
	asmDb  *gorm.DB
	Logger *log.Logger
}

func NewRepository(logger *log.Logger, webDb *gorm.DB, asmDb *gorm.DB) *Repository {
	return &Repository{
		webDb:  webDb,
		asmDb:  asmDb,
		Logger: logger,
	}
}

func (r *Repository) DB(ctx context.Context) *gorm.DB {
	if v := ctx.Value(ctxTxKey); v != nil {
		if tx, ok := v.(*gorm.DB); ok {
			return tx
		}
	}
	if v := ctx.Value(CtxDbKey); v != nil {
		if db, ok := v.(string); ok {
			switch db {
			case config.WebDB:
				return r.webDb.WithContext(ctx)
			case config.AsmDB:
				return r.asmDb.WithContext(ctx)
			}
		}
	}
	if r.asmDb != nil {
		return r.asmDb.WithContext(ctx)
	}
	return r.webDb.WithContext(ctx)
}

func (r *Repository) Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	v := ctx.Value(CtxDbKey)
	if v != nil {
		if db, ok := v.(string); ok {
			switch db {
			case config.WebDB:
				return r.webDb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
					ctx = context.WithValue(ctx, ctxTxKey, tx)
					return fn(ctx)
				})
			case config.AsmDB:
				return r.asmDb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
					ctx = context.WithValue(ctx, ctxTxKey, tx)
					return fn(ctx)
				})
			}
		}
	}
	if r.asmDb != nil {
		return r.asmDb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			ctx = context.WithValue(ctx, ctxTxKey, tx)
			return fn(ctx)
		})
	}
	return r.webDb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ctx = context.WithValue(ctx, ctxTxKey, tx)
		return fn(ctx)
	})
}

func NewDB(conf *config.DBConnection) *gorm.DB {
	if conf == nil {
		panic("db config is nil")
	}
	if conf.Driver == "" || conf.DSN == "" {
		panic("db driver or dsn is empty")
	}

	var (
		db  *gorm.DB
		err error
	)

	switch conf.Driver {
	case "mysql":
		db, err = gorm.Open(mysql.Open(conf.DSN), &gorm.Config{})
	case "postgres":
		db, err = gorm.Open(postgres.New(postgres.Config{
			DSN:                  conf.DSN,
			PreferSimpleProtocol: true,
		}), &gorm.Config{})
	case "sqlite":
		db, err = gorm.Open(sqlite.Open(conf.DSN), &gorm.Config{})
	default:
		panic(fmt.Sprintf("unknown db driver: %s", conf.Driver))
	}
	if err != nil {
		panic(err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		panic(err)
	}
	sqlDB.SetMaxIdleConns(conf.MaxIdleConns)
	sqlDB.SetMaxOpenConns(conf.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(time.Duration(conf.MaxLifeTime) * time.Minute)
	return db
}
