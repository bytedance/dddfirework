package sql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/dddfirework"
	exec_mysql "github.com/bytedance/dddfirework/executor/sql"
	"github.com/bytedance/dddfirework/testsuit"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func initModel(db *gorm.DB) {
	if err := db.AutoMigrate(&TimerJob{}); err != nil {
		panic(err)
	}
}

func init() {
	db := testsuit.InitMysql()
	initModel(db)

	db.Where("1 = 1").Delete(&TimerJob{})
}

func TestTimerConcurrent(t *testing.T) {
	db := testsuit.InitMysql()

	var output string
	var times []time.Time
	var mu sync.Mutex

	testName := "test_concurrent"

	callback := func(ctx context.Context, key, cron string, data []byte) error {
		mu.Lock()
		defer mu.Unlock()

		times = append(times, time.Now())
		output = string(data)
		return nil
	}
	ctx := context.Background()
	var timer *DBTimer
	for i := 0; i < 5; i++ {
		timer = NewDBTimer(testName, db, func(opt *Options) {
			opt.RunInterval = time.Millisecond * 10
		})
		timer.RegisterTimerHandler(callback)
		timer.Start(ctx)
	}

	err := timer.RunCron(testName, "0/1 * * * * ?", []byte(testName))
	assert.NoError(t, err)

	time.Sleep(time.Second * 5)

	assert.Equal(t, testName, output)
	assert.Greater(t, len(times), 2)
	for i := 0; i < len(times)-1; i++ {
		assert.GreaterOrEqual(t, times[i+1].Sub(times[i]), time.Millisecond*950)
		assert.Less(t, times[i+1].Sub(times[i]), time.Millisecond*1050)
	}
}

func TestTimerEngine(t *testing.T) {
	db := testsuit.InitMysql()
	ctx := context.Background()

	testName := "test_engine"
	timer := NewDBTimer(testName, db, func(opt *Options) {
		opt.RunInterval = time.Second
	})
	timer.Start(ctx)

	var k, c string
	engine := dddfirework.NewEngine(nil, exec_mysql.NewExecutor(db), dddfirework.WithTimer(timer))
	engine.RegisterCronTask(dddfirework.EventType(testName), "0/1 * * * * ?", func(key, cron string) {
		k, c = key, cron
	})

	time.Sleep(time.Second * 2)

	assert.Equal(t, testName, k)
	assert.Equal(t, "0/1 * * * * ?", c)
}

func TestTimerCancel(t *testing.T) {
	db := testsuit.InitMysql()

	var output string
	testName := "test_cancel"

	ctx := context.Background()
	var timer = NewDBTimer(testName, db, func(opt *Options) {
		opt.RunInterval = time.Second
	})
	timer.RegisterTimerHandler(func(ctx context.Context, key, cron string, data []byte) error {
		output = string(data)
		return nil
	})
	timer.Start(ctx)

	err := timer.RunCron(testName, "0/1 * * * * ?", []byte(testName))
	assert.NoError(t, err)

	time.Sleep(time.Second * 2)

	assert.Equal(t, testName, output)

	err = timer.Cancel(testName)
	assert.NoError(t, err)

	output = ""
	time.Sleep(time.Second * 2)
	assert.Empty(t, output)
}

func TestTimerOnce(t *testing.T) {
	db := testsuit.InitMysql()

	var output string
	testName := "test_once"

	ctx := context.Background()
	var timer = NewDBTimer(testName, db, func(opt *Options) {
		opt.RunInterval = time.Millisecond * 500
	})
	timer.RegisterTimerHandler(func(ctx context.Context, key, cron string, data []byte) error {
		output = string(data)
		return nil
	})
	timer.Start(ctx)

	err := timer.RunOnce(testName, time.Now().Add(time.Millisecond*500), []byte(testName))
	assert.NoError(t, err)

	time.Sleep(time.Second * 1)

	assert.Equal(t, testName, output)

	output = ""
	time.Sleep(time.Second * 2)
	assert.Empty(t, output)
}
