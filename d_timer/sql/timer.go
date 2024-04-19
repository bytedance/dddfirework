package sql

import (
	"context"
	stdlog "log"
	"os"
	"sync"
	"time"

	ddd "github.com/bytedance/dddfirework"
	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const defaultInterval = time.Second

var defaultLogger = stdr.New(stdlog.New(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)).WithName("db_timer")

type TimerStatus int

const (
	TimerToRun    TimerStatus = 1
	TimerFinished TimerStatus = 2
	TimerFailed   TimerStatus = 3
)

type Options struct {
	RunInterval time.Duration
	Logger      logr.Logger
}

type Option func(opt *Options)

type DBTimer struct {
	service string
	db      *gorm.DB
	cb      ddd.TimerHandler
	opt     Options
	logger  logr.Logger
	once    sync.Once
}

func NewDBTimer(service string, db *gorm.DB, opts ...Option) *DBTimer {
	if service == "" {
		panic("service name is required")
	}
	opt := Options{
		RunInterval: defaultInterval,
		Logger:      defaultLogger,
	}
	for _, o := range opts {
		o(&opt)
	}
	return &DBTimer{
		service: service,
		db:      db,
		opt:     opt,
		logger:  opt.Logger,
		once:    sync.Once{},
	}
}

func (t *DBTimer) RunCron(key, cronExp string, data []byte) error {
	newTimer := TimerJob{
		Service: t.service,
		Key:     key,
		Cron:    cronExp,
		Payload: data,
		Status:  TimerToRun,
	}
	return t.run(&newTimer)
}

func (t *DBTimer) RunOnce(key string, runTime time.Time, data []byte) error {
	if runTime.Before(time.Now()) {
		return ErrTimerOverdue
	}

	newTimer := TimerJob{
		Service:  t.service,
		Key:      key,
		NextTime: runTime,
		Payload:  data,
		Status:   TimerToRun,
	}
	return t.run(&newTimer)
}

func (t *DBTimer) Cancel(key string) error {
	return t.db.Unscoped().Where(TimerJob{Key: key}).Delete(&TimerJob{}).Error
}

func (t *DBTimer) run(job *TimerJob) error {
	if err := job.Reset(); err != nil {
		return err
	}
	return t.db.Where(TimerJob{
		Service: t.service,
		Key:     job.Key,
	}).Attrs(job).FirstOrCreate(&TimerJob{}).Error
}

func (t *DBTimer) RegisterTimerHandler(cb ddd.TimerHandler) {
	t.cb = cb
}

func (t *DBTimer) handleJobs(ctx context.Context) error {
	return t.db.Transaction(func(tx *gorm.DB) error {
		jobs := make([]*TimerJob, 0)
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"service = ? and next_time <= ? and status = ?", t.service, time.Now(), TimerToRun,
		).Find(&jobs).Error; err != nil {
			return err
		}

		if len(jobs) == 0 {
			return nil
		}

		for _, job := range jobs {
			// 拆分job到独立的事务，避免长事务锁定大量资源。事务传播行为：REQUIRES_NEW
			if err := t.db.Transaction(func(jobTx *gorm.DB) error {
				if err := t.cb(ctx, job.Key, job.Cron, job.Payload); err != nil {
					t.logger.Error(err, "timer callback failed")
				}
				if err := job.Next(); err != nil {
					job.Close(err)
				}

				// 保存job，避免重复执行
				if err := jobTx.Save(job).Error; err != nil {
					return err
				}

				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})

}

func (t *DBTimer) Start(ctx context.Context) {
	t.once.Do(func() {
		run := func() {
			// 定时触发event_handler
			ticker := time.NewTicker(t.opt.RunInterval)
			for range ticker.C {
				if err := t.handleJobs(context.Background()); err != nil {
					t.logger.Error(err, "handle job failed")
				}
			}
		}
		go run()
	})
}
