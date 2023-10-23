package sql

import (
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron"
)

var ErrTimerOverdue = fmt.Errorf("timer overdue")

type TimerJob struct {
	ID        int64       `gorm:"primaryKey;column:id;autoIncrement"`
	Service   string      `gorm:"column:service;type:varchar(30)"`
	Key       string      `gorm:"column:key;type:varchar(30);uniqueIndex;not null"`
	Cron      string      `gorm:"column:cron;type:varchar(30);null"`
	NextTime  time.Time   `gorm:"column:next_time;type:datetime;index;not null"`
	Status    TimerStatus `gorm:"column:status;type:tinyint"`
	Msg       string      `gorm:"column:msg;type:varchar(128)"`
	Payload   []byte      `gorm:"column:payload;type:text"`
	CreatedAt time.Time   `gorm:"index;type:datetime"`
}

func (t *TimerJob) TableName() string {
	return "ddd_timer"
}

func (t *TimerJob) Next() error {
	if t.Cron != "" {
		return t.Reset()
	} else {
		t.Close(nil)
		return nil
	}
}

func (t *TimerJob) Reset() error {
	if t.Cron == "" {
		return nil
	}
	scheduler, err := cron.Parse(t.Cron)
	if err != nil {
		t.Close(err)
		return err
	}

	t.NextTime = scheduler.Next(time.Now())
	t.Status = TimerToRun
	return nil
}

func (t *TimerJob) Close(err error) {
	if err == nil || errors.Is(err, ErrTimerOverdue) {
		t.Status = TimerFinished
	} else {
		t.Status = TimerFailed
		t.Msg = err.Error()
	}
}
