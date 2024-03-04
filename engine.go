//
// Copyright 2023 Bytedance Ltd. and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dddfirework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	"github.com/rs/xid"

	"github.com/bytedance/dddfirework/logger/stdr"
)

var ErrBreak = fmt.Errorf("break process") // 中断流程，不返回错误
var ErrEntityNotFound = fmt.Errorf("entity not found")
var ErrEntityRepeated = fmt.Errorf("entity already added")

var defaultLogger = stdr.NewStdr("ddd_engine")

type ILock interface {
	Lock(ctx context.Context, key string) (keyLock interface{}, err error)
	UnLock(ctx context.Context, keyLock interface{}) error
}

type IIDGenerator interface {
	NewID() (string, error)
}

type defaultIDGenerator struct {
}

func (d *defaultIDGenerator) NewID() (string, error) {
	guid := xid.New()
	return guid.String(), nil
}

// EntityContainer 负责维护领域内所有聚合根实体的实体
type EntityContainer struct {
	BaseEntity

	roots   []IEntity // 保存聚合根实体
	deleted []IEntity // 保存所有被删除实体
}

func (w *EntityContainer) GetChildren() map[string][]IEntity {
	return map[string][]IEntity{"meta": w.roots}
}

func (w *EntityContainer) Has(root IEntity) bool {
	for _, r := range w.roots {
		if r == root {
			return true
		}
	}
	return false
}

func (w *EntityContainer) GetDeleted() []IEntity {
	return w.deleted
}

func (w *EntityContainer) SetChildren(roots []IEntity) {
	// EntityContainer 里面会有修改 roots 的操作，应当拷贝一个新的 slice，隔离输入的影响
	w.roots = make([]IEntity, len(roots))
	copy(w.roots, roots)
}

func (w *EntityContainer) Add(root IEntity) error {
	for _, e := range w.roots {
		if e == root {
			return ErrEntityRepeated
		}
	}

	w.roots = append(w.roots, root)
	return nil
}

func (w *EntityContainer) Remove(root IEntity) error {
	i := 0
	for _, item := range w.roots {
		if item != root {
			w.roots[i] = item
			i++
		}
	}
	if i == len(w.roots) {
		return ErrEntityNotFound
	}
	w.roots = w.roots[:i]
	w.deleted = append(w.deleted, root)
	return nil
}

// Recycle 回收所有被删除的实体
func (w *EntityContainer) Recycle(e IEntity) {
	w.deleted = append(w.deleted, e)
}

type ErrList []error

func (e ErrList) Error() string {
	errs := make([]string, 0)
	for _, err := range e {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

type Result struct {
	Error   error
	Break   bool
	Actions []*Action
	Output  interface{}
}

func ResultErrors(err ...error) *Result {
	return &Result{Error: ErrList(err)}
}

func ResultError(err error) *Result {
	return &Result{Error: err}
}

func ResultErrOrBreak(err error) *Result {
	if errors.Is(err, ErrBreak) {
		return &Result{Break: true}
	}
	return ResultError(err)
}

type DomainBuilder struct {
	stage *Stage
}

// Build 查询并构建 parent 以及 children 实体
// parent 必须指定 id，children 为可选，需要是 *IEntity 或者 *[]IEntity 类型
func (h DomainBuilder) Build(ctx context.Context, parent IEntity, children ...interface{}) error {
	return h.stage.BuildEntity(ctx, parent, children...)
}

// RootContainer 聚合根实体容器
type RootContainer struct {
	stage *Stage
	errs  []error
}

// Add 创建聚合根实体
func (h *RootContainer) Add(root IEntity) {
	if err := h.stage.meta.Add(root); err != nil {
		h.errs = append(h.errs, err)
	}
}

// Remove 删除聚合根实体
func (h *RootContainer) Remove(root IEntity) {
	if err := h.stage.meta.Remove(root); err != nil {
		h.errs = append(h.errs, err)
	}
}

// Repository 聚合根实体仓库
type Repository struct {
	stage *Stage
	errs  []error
}

func (r *Repository) appendError(e error) {
	r.errs = append(r.errs, e)
}

func (r *Repository) getError() error {
	return errors.Join(r.errs...)
}

// Get 查询并构建聚合根
// root 必须指定 id，children 为可选，是 root 下面子实体的指针，需要是 *IEntity 或者 *[]IEntity 类型
// 方法会根据 root 与 children 的关系，查询并构建 root 与 children 实体
func (r *Repository) Get(ctx context.Context, root IEntity, children ...interface{}) error {
	if r.stage.hasSnapshot(root) {
		return fmt.Errorf("entity has added")
	}

	if err := r.stage.BuildEntity(ctx, root, children...); err != nil {
		return err
	}

	if err := r.stage.meta.Add(root); err != nil {
		return err
	}
	return r.stage.updateSnapshot()
}

// CustomGet 自定义函数获取聚合根实体，并添加到快照
func (r *Repository) CustomGet(ctx context.Context, getter func(ctx context.Context, root ...IEntity), roots ...IEntity) error {
	getter(ctx, roots...)

	for _, root := range roots {
		if r.stage.hasSnapshot(root) {
			return fmt.Errorf("entity has added")
		}
		if err := r.stage.meta.Add(root); err != nil {
			return err
		}
	}

	return r.stage.updateSnapshot()
}

// Add 添加聚合根到仓库
func (r *Repository) Add(roots ...IEntity) {
	for _, root := range roots {
		if r.stage.hasSnapshot(root) {
			r.appendError(fmt.Errorf("root must be a new entity"))
			return
		}
		if err := r.stage.meta.Add(root); err != nil {
			r.appendError(err)
			return
		}
	}
}

// Remove 删除聚合根，root.GetID 不能为空
func (r *Repository) Remove(roots ...IEntity) {
	toCreate := make([]IEntity, 0)
	for _, root := range roots {
		if !r.stage.meta.Has(root) {
			toCreate = append(toCreate, root)
		}
	}
	if len(toCreate) > 0 {
		r.Add(toCreate...)
		if err := r.stage.updateSnapshot(); err != nil {
			r.appendError(err)
			return
		}
	}

	for _, root := range roots {
		if err := r.stage.meta.Remove(root); err != nil {
			r.appendError(err)
			return
		}
	}
}

// Save 执行一次保存，并刷新快照
func (r *Repository) Save(ctx context.Context) error {
	if err := r.getError(); err != nil {
		return err
	}
	return r.stage.commit(ctx)
}

type BuildFunc func(ctx context.Context, h DomainBuilder) (roots []IEntity, err error)
type ActFunc func(ctx context.Context, container RootContainer, roots ...IEntity) error
type MainFunc func(ctx context.Context, repo *Repository) error
type PostSaveFunc func(ctx context.Context, res *Result)

// EventHandlerConstruct EventHandler 的构造函数，带一个入参和一个返回值，入参是与事件类型匹配的事件数据指针类型，
// 返回值支持三种:
// - ICommand interface
// - ICommandMain interface
// - MainFunc type
// 示例 func(evt *OrderCreatedEvent) *OnEventCreateCommand
type EventHandlerConstruct interface{}

type Options struct {
	WithTransaction bool
	RecursiveDelete bool // 删除根实体是否递归删除所有子实体
	DryRun          bool // dryrun 模式，不执行持久化，不发送事件
	Locker          ILock
	Executor        IExecutor
	EventPersist    EventPersist // 是否保存领域事件到 DB
	Logger          logr.Logger
	EventBus        IEventBus
	Timer           ITimer
	IDGenerator     IIDGenerator
	PostSaveHooks   []PostSaveFunc
}

type Option interface {
	ApplyToOptions(*Options)
}
type TransactionOption bool

func (t TransactionOption) ApplyToOptions(opts *Options) {
	opts.WithTransaction = bool(t)
}

const WithTransaction = TransactionOption(true)
const WithoutTransaction = TransactionOption(false)

type RecursiveDeleteOption bool

func (t RecursiveDeleteOption) ApplyToOptions(opts *Options) {
	opts.RecursiveDelete = bool(t)
}

const WithRecursiveDelete = RecursiveDeleteOption(true)

type DryRunOption bool

func (t DryRunOption) ApplyToOptions(opts *Options) {
	opts.DryRun = bool(t)
}

const WithDryRun = DryRunOption(true)

type LoggerOption struct {
	logger logr.Logger
}

func (t LoggerOption) ApplyToOptions(opts *Options) {
	opts.Logger = t.logger
}

func WithLogger(logger logr.Logger) LoggerOption {
	return LoggerOption{logger: logger}
}

type LockOption struct {
	lock ILock
}

func (t LockOption) ApplyToOptions(opts *Options) {
	opts.Locker = t.lock
}

func WithLock(lock ILock) LockOption {
	return LockOption{lock: lock}
}

type ExecutorOption struct {
	executor IExecutor
}

func (t ExecutorOption) ApplyToOptions(opts *Options) {
	opts.Executor = t.executor
}

func WithExecutor(executor IExecutor) ExecutorOption {
	return ExecutorOption{executor: executor}
}

type EventBusOption struct {
	eventBus IEventBus
}

func (t EventBusOption) ApplyToOptions(opts *Options) {
	opts.EventBus = t.eventBus
}

func WithEventBus(eventBus IEventBus) EventBusOption {
	return EventBusOption{eventBus: eventBus}
}

type DTimerOption struct {
	timer ITimer
}

func (t DTimerOption) ApplyToOptions(opts *Options) {
	opts.Timer = t.timer
}

func WithTimer(timer ITimer) DTimerOption {
	return DTimerOption{timer: timer}
}

type EventPersist func(event *DomainEvent) (IModel, error)

type EventSaveOption EventPersist

func (t EventSaveOption) ApplyToOptions(opts *Options) {
	opts.EventPersist = EventPersist(t)
}

func WithEventPersist(f EventPersist) EventSaveOption {
	return EventSaveOption(f)
}

type IDGeneratorOption struct {
	idGen IIDGenerator
}

func (t IDGeneratorOption) ApplyToOptions(opts *Options) {
	opts.IDGenerator = t.idGen
}

func WithIDGenerator(idGen IIDGenerator) IDGeneratorOption {
	return IDGeneratorOption{idGen: idGen}
}

type PostSaveOption PostSaveFunc

func (t PostSaveOption) ApplyToOptions(opts *Options) {
	opts.PostSaveHooks = append(opts.PostSaveHooks, PostSaveFunc(t))
}

func WithPostSave(f PostSaveFunc) PostSaveOption {
	return PostSaveOption(f)
}

type Engine struct {
	locker      ILock
	executor    IExecutor
	idGenerator IIDGenerator
	eventbus    IEventBus
	timer       ITimer
	logger      logr.Logger
	options     Options
}

func NewEngine(l ILock, e IExecutor, opts ...Option) *Engine {
	options := Options{
		Locker:   l,
		Executor: e,

		// 默认开启事务
		WithTransaction: true,
		Logger:          defaultLogger,
		IDGenerator:     &defaultIDGenerator{},
		EventBus:        &noEventBus{},
		Timer:           &noTimer{},
	}
	for _, opt := range opts {
		opt.ApplyToOptions(&options)
	}
	eventBus := options.EventBus
	eventBus.RegisterEventHandler(onEvent)
	if txEB, ok := eventBus.(ITransactionEventBus); ok {
		txEB.RegisterEventTXChecker(onTXChecker)
	}
	timer := options.Timer
	timer.RegisterTimerHandler(onTimer)
	return &Engine{
		locker:      options.Locker,
		executor:    options.Executor,
		eventbus:    eventBus,
		timer:       timer,
		options:     options,
		logger:      options.Logger,
		idGenerator: options.IDGenerator,
	}
}

func (e *Engine) NewStage() *Stage {
	return &Stage{
		locker:      e.locker,
		executor:    e.executor,
		eventBus:    e.eventbus,
		timer:       e.timer,
		idGenerator: e.idGenerator,
		meta:        &EntityContainer{},
		snapshot:    map[IEntity]*entitySnapshot{},
		result:      &Result{},
		options:     e.options,
		logger:      e.logger,
	}
}

func (e *Engine) Create(ctx context.Context, roots ...IEntity) *Result {
	return e.NewStage().Main(func(ctx context.Context, repo *Repository) error {
		repo.Add(roots...)
		return nil
	}).Save(ctx)
}

func (e *Engine) Delete(ctx context.Context, roots ...IEntity) *Result {
	return e.NewStage().Main(func(ctx context.Context, repo *Repository) error {
		repo.Remove(roots...)
		return nil
	}).Save(ctx)
}

// Deprecated: 请用 Run 方法代替
func (e *Engine) RunCommand(ctx context.Context, c ICommand, opts ...Option) *Result {
	return e.NewStage().WithOption(opts...).RunCommand(ctx, c)
}

// Run 运行命令，支持以下格式：
// 实现 ICommand 接口的对象
// 实现 ICommandMain 接口的对象
// 类型为 func(ctx context.Context, repo Repository) error 的函数
func (e *Engine) Run(ctx context.Context, c interface{}, opts ...Option) *Result {
	return e.NewStage().WithOption(opts...).Run(ctx, c)
}

func (e *Engine) RegisterEventHandler(eventType EventType, construct EventHandlerConstruct) {
	handlerType := reflect.TypeOf(construct)
	if handlerType.Kind() != reflect.Func {
		panic("construct must type of reflect.Func")
	}
	if handlerType.NumIn() != 1 || handlerType.NumOut() != 1 {
		panic("construct num of arg or output must 1")
	}

	evtType := handlerType.In(0)
	if evtType.Kind() != reflect.Ptr {
		panic("event type must be pointer")
	}
	evtType = evtType.Elem() // event type 引用实际类型
	constructFunc := reflect.ValueOf(construct)

	RegisterEventHandler(eventType, func(ctx context.Context, evt *DomainEvent) error {
		var bizEvt reflect.Value
		if evtType == domainEventType {
			bizEvt = reflect.ValueOf(evt)
		} else {
			bizEvt = reflect.New(evtType)
			if err := json.Unmarshal(evt.Payload, bizEvt.Interface()); err != nil {
				e.logger.Error(err, "unmarshal event failed")
				return err
			}
		}

		outputs := constructFunc.Call([]reflect.Value{bizEvt})

		if res := e.Run(ctx, outputs[0].Interface()); res.Error != nil {
			e.logger.Error(res.Error, "event handler exec failed")
			return res.Error
		}
		return nil
	})
}

// RegisterCronTask 注册定时任务
func (e *Engine) RegisterCronTask(key EventType, cron string, f func(key, cron string)) {
	if e.timer == nil {
		panic("No ITimer specified")
	}
	if hasEventHandler(key) {
		panic("key has registered")
	}

	RegisterEventHandler(key, func(ctx context.Context, evt *TimerEvent) error {
		f(evt.Key, evt.Cron)
		return nil
	})

	if err := e.timer.RunCron(string(key), cron, nil); err != nil {
		panic(err)
	}
}

// RegisterCronTaskOfCommand 注册定时触发的 ICommand
func (e *Engine) RegisterCronTaskOfCommand(key EventType, cron string, f func(key, cron string) ICommand) {
	if e.timer == nil {
		panic("No ITimer specified")
	}
	if hasEventHandler(key) {
		panic("key has registered")
	}

	e.RegisterEventHandler(key, func(evt *TimerEvent) ICommand {
		return f(evt.Key, evt.Cron)
	})
	if err := e.timer.RunCron(string(key), cron, nil); err != nil {
		panic(err)
	}
}

// Stage 取舞台的意思，表示单次运行
type Stage struct {
	lockKeys []string
	main     MainFunc

	locker      ILock
	executor    IExecutor
	eventBus    IEventBus
	timer       ITimer
	idGenerator IIDGenerator
	logger      logr.Logger
	options     Options

	meta     *EntityContainer
	snapshot entitySnapshotPool
	result   *Result
	eventCtx context.Context
}

func (e *Stage) WithOption(opts ...Option) *Stage {
	for _, opt := range opts {
		opt.ApplyToOptions(&e.options)
	}

	eventBus := e.options.EventBus
	eventBus.RegisterEventHandler(onEvent)
	if txEB, ok := eventBus.(ITransactionEventBus); ok {
		txEB.RegisterEventTXChecker(onTXChecker)
	}
	e.eventBus = eventBus

	timer := e.options.Timer
	timer.RegisterTimerHandler(onTimer)
	e.locker = e.options.Locker
	e.executor = e.options.Executor
	e.timer = timer
	e.logger = e.options.Logger
	e.idGenerator = e.options.IDGenerator
	return e
}

func changeType2OP(t changeType) OpType {
	switch t {
	case newChildren:
		return OpInsert
	case dirtyChildren:
		return OpUpdate
	case deleteChildren, clearChildren:
		return OpDelete
	}
	return OpUnknown
}

// BuildEntity 查询并构建 parent 以及 children 实体
// parent 必须指定 id，children 为可选，需要是 *IEntity 或者 *[]IEntity 类型
func (e *Stage) BuildEntity(ctx context.Context, parent IEntity, children ...interface{}) error {
	if parent.GetID() == "" {
		return fmt.Errorf("parent must has ID")
	}

	if err := e.buildEntity(ctx, parent, nil); err != nil {
		return err
	}

	for _, item := range children {
		itemType := reflect.TypeOf(item)
		if itemType.Kind() != reflect.Ptr {
			return fmt.Errorf("children must be pointer")
		}
		if itemType.Elem().Kind() == reflect.Slice {
			if err := e.buildEntitySliceByParent(ctx, parent, item); err != nil {
				return err
			}
		} else if itemType.Implements(entityType) {
			if err := e.buildEntity(ctx, item.(IEntity), parent); err != nil && !errors.Is(err, ErrEntityNotFound) {
				return err
			}
		} else {
			return fmt.Errorf("children type must be IEntity")
		}
	}
	return nil
}

// 查询并构建 entity 实体，注意，不会处理 parent 实体
func (e *Stage) buildEntity(ctx context.Context, entity, parent IEntity) error {
	// 至少一个有 ID
	if entity.GetID() == "" && parent.GetID() == "" {
		return fmt.Errorf("entity to build must has id")
	}
	po, err := e.executor.Entity2Model(entity, parent, OpQuery)
	if err != nil {
		return err
	}
	posPointer := reflect.New(reflect.SliceOf(reflect.TypeOf(po)))
	if err := e.executor.Exec(ctx, &Action{
		Op:          OpQuery,
		Query:       po,
		QueryResult: posPointer.Interface(),
	}); err != nil {
		return err
	}
	if posPointer.Elem().Len() == 0 {
		return ErrEntityNotFound
	}
	queryPO := posPointer.Elem().Index(0).Interface()
	return e.executor.Model2Entity(queryPO.(IModel), entity)
}

func (e *Stage) buildEntitySliceByParent(ctx context.Context, parent IEntity, children interface{}) error {
	childrenType := reflect.TypeOf(children)
	if childrenType.Kind() != reflect.Ptr || childrenType.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("children must be pointer of slice")
	}
	eType := childrenType.Elem().Elem()
	if !eType.Implements(entityType) {
		return fmt.Errorf("element of children must implement IEntity")
	}

	if eType.Kind() == reflect.Ptr {
		eType = eType.Elem()
	}
	entity := reflect.New(eType)
	po, err := e.executor.Entity2Model(entity.Interface().(IEntity), parent, OpQuery)
	if err != nil {
		return err
	}
	posPointer := reflect.New(reflect.SliceOf(reflect.TypeOf(po)))

	if err := e.executor.Exec(ctx, &Action{
		Op:          OpQuery,
		Query:       po,
		QueryResult: posPointer.Interface(),
	}); err != nil {
		return err
	}
	if posPointer.Elem().Len() == 0 {
		return nil
	}

	resultVal := reflect.ValueOf(children)
	entitiesVal := resultVal.Elem()
	for i := 0; i < posPointer.Elem().Len(); i++ {
		newEntity := reflect.New(eType)
		if err := e.executor.Model2Entity(posPointer.Elem().Index(i).Interface().(IModel), newEntity.Interface().(IEntity)); err != nil {
			return err
		}
		entitiesVal = reflect.Append(entitiesVal, newEntity)
	}
	resultVal.Elem().Set(entitiesVal)
	return nil
}

func (e *Stage) Lock(keys ...string) *Stage {
	e.lockKeys = keys
	return e
}

func (e *Stage) Main(f MainFunc) *Stage {
	e.main = f
	return e
}

// Deprecated: 请用 Run 方法代替
func (e *Stage) RunCommand(ctx context.Context, c ICommand) *Result {
	return e.Run(ctx, c)
}

func (e *Stage) runCommand(ctx context.Context, c ICommand) *Result {
	if setter, ok := c.(IStageSetter); ok {
		setter.SetStage(StageAgent{st: e})
	}

	keys, err := c.Init(ctx)
	if err != nil {
		return ResultErrOrBreak(err)
	}
	return e.WithOption(PostSaveOption(c.PostSave)).Lock(keys...).Main(func(ctx context.Context, repo *Repository) error {
		buildRoots, err := c.Build(ctx, DomainBuilder{stage: repo.stage})
		if err != nil {
			return err
		}
		for _, r := range buildRoots {
			if r.GetID() == "" {
				return fmt.Errorf("build entities must have ID, for create case, just use container.Add(**) at act func")
			}
		}
		repo.stage.meta.SetChildren(buildRoots)

		// 保存父子实体关系链
		if err = repo.stage.flush(); err != nil {
			return err
		}

		container := RootContainer{stage: repo.stage}
		if err := c.Act(ctx, container, buildRoots...); err != nil {
			return err
		} else if len(container.errs) > 0 {
			return ErrList(container.errs)
		}
		return nil
	}).Save(ctx)
}

// Run 运行命令，支持以下格式：
// 实现 ICommand 接口的对象
// 实现 ICommandMain 接口的对象
// 类型为 func(ctx context.Context, repo Repository) error 的函数
func (e *Stage) Run(ctx context.Context, cmd interface{}) *Result {
	switch c := cmd.(type) {
	case ICommand:
		return e.runCommand(ctx, c)
	case ICommandMain:
		var keys []string
		var options []Option
		if cmdInit, ok := cmd.(ICommandInit); ok {
			initKeys, err := cmdInit.Init(ctx)
			if err != nil {
				return ResultErrOrBreak(err)
			}
			keys = initKeys
		}
		if cmdPostSave, ok := cmd.(ICommandPostSave); ok {
			options = append(options, PostSaveOption(cmdPostSave.PostSave))
		}
		return e.WithOption(options...).Lock(keys...).Main(c.Main).Save(ctx)
	case func(ctx context.Context, repo *Repository) error:
		return e.Main(c).Save(ctx)
	case MainFunc:
		return e.Main(c).Save(ctx)
	default:
		panic(fmt.Sprintf("cmd type %T is invalid", c))
	}
}

func childrenSnapshot(children map[string][]IEntity) map[string][]IEntity {
	snapshot := make(map[string][]IEntity, len(children))
	for k, v := range children {
		v2 := make([]IEntity, len(v))
		copy(v2, v)
		snapshot[k] = v2
	}
	return snapshot
}

func (e *Stage) flush() error {
	e.snapshot = entitySnapshotPool{}
	return e.updateSnapshot()
}

// 更新快照，已有的不覆盖
func (e *Stage) updateSnapshot() error {
	return walk(e.meta, nil, func(entity, parent IEntity, children map[string][]IEntity) error {
		if _, in := e.snapshot[entity]; in && entity != e.meta {
			return nil
		}

		po, err := e.executor.Entity2Model(entity, parent, OpQuery)
		if err != nil && !errors.Is(ErrEntityNotRegister, err) {
			return err
		}
		e.snapshot[entity] = &entitySnapshot{
			po:       po,
			children: childrenSnapshot(children),
		}
		return nil
	})
}

func (e *Stage) hasSnapshot(target IEntity) bool {
	return e.snapshot[target] != nil
}

// unDirty 对所有实体取消 Dirty 标记
func (e *Stage) unDirty() {
	_ = walk(e.meta, nil, func(entity, parent IEntity, children map[string][]IEntity) error {
		entity.UnDirty()
		return nil
	})
}

func (e *Stage) getEntityChanged() ([]*entityChanged, error) {
	changed := entityDiff(e.meta, e.snapshot)
	// 处理实体移动的场景
	changed, err := handleEntityMove(changed)
	if err != nil {
		return nil, err
	}
	// 处理递归删除子实体
	if e.options.RecursiveDelete {
		changed = recursiveDelete(changed)
	}
	return changed, nil
}

func (e *Stage) recycle(changed []*entityChanged) {
	for _, c := range changed {
		if c.changeType == deleteChildren {
			for _, entity := range c.children {
				e.meta.Recycle(entity)
			}
		}
	}
}

// 为新对象统一生成id
func (e *Stage) putNewID(changes []*entityChanged) error {
	for _, item := range changes {
		if item.changeType == newChildren {
			for _, child := range item.children {
				if child.GetID() == "" {
					id, err := e.idGenerator.NewID()
					if err != nil {
						return err
					}
					child.SetID(id)
				}
			}
		}
	}
	return nil
}

// 相同操作，相同类型的PO，合并到一个 Action
func (e *Stage) makeActions(changes []*entityChanged) ([]*Action, error) {
	typeActions := make(map[OpType]map[reflect.Type]*Action, 3)
	poTypes := []reflect.Type{}
	for _, item := range changes {
		for _, entity := range item.children {
			op := changeType2OP(item.changeType)
			po, err := e.executor.Entity2Model(entity, item.parent, op)
			if err != nil {
				if errors.Is(err, ErrEntityNotRegister) {
					e.logger.Info("entity not registered", "type", reflect.TypeOf(entity))
					continue
				}
				return nil, err
			}
			poType := reflect.TypeOf(po)
			if _, in := typeActions[op]; !in {
				poTypes = append(poTypes, poType)
				typeActions[op] = map[reflect.Type]*Action{}
			}

			if _, in := typeActions[op][poType]; in {
				typeActions[op][poType].Models = append(typeActions[op][poType].Models, po)
			} else {
				typeActions[op][poType] = &Action{
					Op:     op,
					Models: []IModel{po},
				}
			}
			if op == OpUpdate {
				typeActions[op][poType].PrevModels = append(typeActions[op][poType].PrevModels, e.snapshot[entity].po)
			}
		}
	}
	actions := make([]*Action, 0)
	for _, t := range []OpType{OpInsert, OpUpdate, OpDelete} {
		for _, at := range poTypes {
			if _, in := typeActions[t][at]; !in {
				continue
			}
			actions = append(actions, typeActions[t][at])
			delete(typeActions[t], at)
		}
	}
	return actions, nil
}

func (e *Stage) collectEvents() []*DomainEvent {
	eventMap := make(map[string]*DomainEvent, 0)
	_ = walk(e.meta, nil, func(entity, parent IEntity, children map[string][]IEntity) (err error) {
		for _, evt := range entity.GetEvents() {
			eventMap[evt.ID] = evt
		}
		return
	})
	// 收集已删除的实体发送的事件
	for _, del := range e.meta.GetDeleted() {
		_ = walk(del, nil, func(entity, parent IEntity, children map[string][]IEntity) (err error) {
			for _, evt := range entity.GetEvents() {
				eventMap[evt.ID] = evt
			}
			return
		})
	}
	events := make([]*DomainEvent, 0)
	for _, evt := range eventMap {
		events = append(events, evt)
	}
	// 事件根据发送时间 + id 的顺序排序，id 由 xid 保证单节点严格自增
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].CreatedAt.Before(events[j].CreatedAt) ||
			(events[i].CreatedAt.Equal(events[j].CreatedAt) && events[i].ID < events[j].ID)
	})
	return events
}

func (e *Stage) makeEventPersistAction(events []*DomainEvent) (*Action, error) {
	pos := make([]IModel, len(events))
	for i, evt := range events {
		po, err := e.options.EventPersist(evt)
		if err != nil {
			return nil, err
		}
		pos[i] = po
	}
	return &Action{
		Op:         OpInsert,
		Models:     pos,
		PrevModels: []IModel{},
	}, nil
}

func (e *Stage) dispatchEvents(ctx context.Context, events []*DomainEvent) (err error) {
	if e.options.DryRun {
		return nil
	}
	if !e.options.WithTransaction {
		e.logger.Info("engine not support transaction")
		return e.eventBus.Dispatch(ctx, events...)
	}

	normalEvents, txEvents := make([]*DomainEvent, 0), make([]*DomainEvent, 0)
	for _, evt := range events {
		if evt.SendType == SendTypeTransaction {
			txEvents = append(txEvents, evt)
		} else {
			normalEvents = append(normalEvents, evt)
		}
	}
	if txEventBus, ok := e.eventBus.(ITransactionEventBus); ok {
		if len(txEvents) > 0 {
			e.eventCtx, err = txEventBus.DispatchBegin(ctx, txEvents...)
			if err != nil {
				return err
			}
		}

		if len(normalEvents) > 0 {
			if err = txEventBus.Dispatch(ctx, normalEvents...); err != nil {
				return err
			}
		}
		return
	} else {
		// 如果 eventbus 不支持事务，所有事件默认按照普通方式发送
		return e.eventBus.Dispatch(ctx, events...)
	}
}

func (e *Stage) commit(ctx context.Context) error {
	if err := e.persist(ctx); err != nil {
		return err
	}
	if err := e.flush(); err != nil {
		return err
	}

	e.unDirty()
	return nil
}

func (e *Stage) persist(ctx context.Context) error {
	// 发现实体变更
	changed, err := e.getEntityChanged()
	if err != nil {
		return err
	}
	// 执行保存前的 hook
	for _, c := range changed {
		for _, entity := range c.children {
			if err := execHook(ctx, entity, c.changeType, true); err != nil {
				return err
			}
		}
	}

	if err := e.putNewID(changed); err != nil {
		return err
	}
	// 转换为持久化 Action 序列
	actions, err := e.makeActions(changed)
	if err != nil {
		return err
	}

	if err := e.exec(ctx, actions); err != nil {
		return err
	}
	// 执行保存后的 hook
	for _, c := range changed {
		for _, entity := range c.children {
			if err := execHook(ctx, entity, c.changeType, false); err != nil {
				return err
			}
		}
	}

	// 回收被删除的子实体，被删除的子实体仍可能有事件需要发送
	e.recycle(changed)
	return nil
}

func execHook(ctx context.Context, entity IEntity, ct changeType, isBefore bool) error {
	if ct == newChildren && isBefore {
		if saver, ok := entity.(IBeforeCreate); ok {
			if err := saver.BeforeCreate(ctx); err != nil {
				return err
			}
		}
	} else if ct == newChildren && !isBefore {
		if saver, ok := entity.(IAfterCreate); ok {
			if err := saver.AfterCreate(ctx); err != nil {
				return err
			}
		}
	} else if ct == dirtyChildren && isBefore {
		if saver, ok := entity.(IBeforeUpdate); ok {
			if err := saver.BeforeUpdate(ctx); err != nil {
				return err
			}
		}
	} else if ct == dirtyChildren && !isBefore {
		if saver, ok := entity.(IAfterUpdate); ok {
			if err := saver.AfterUpdate(ctx); err != nil {
				return err
			}
		}
	} else if ct == deleteChildren && isBefore {
		if saver, ok := entity.(IBeforeDelete); ok {
			if err := saver.BeforeDelete(ctx); err != nil {
				return err
			}
		}
	} else if ct == deleteChildren && !isBefore {
		if saver, ok := entity.(IAfterDelete); ok {
			if err := saver.AfterDelete(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Stage) exec(ctx context.Context, actions []*Action) error {
	if e.options.DryRun {
		return nil
	}
	for _, a := range actions {
		if err := e.executor.Exec(ctx, a); err != nil {
			return err
		}
	}
	e.result.Actions = append(e.result.Actions, actions...)
	return nil
}

func (e *Stage) setOutput(data interface{}) {
	e.result.Output = data
}

func (e *Stage) do(ctx context.Context) *Result {
	// 创建聚合
	var err error
	if e.main != nil {
		repo := &Repository{stage: e}
		if err := e.main(ctx, repo); err != nil {
			return ResultErrOrBreak(err)
		}
		if err := repo.getError(); err != nil {
			return ResultError(err)
		}
	}

	err = e.persist(ctx)
	if err != nil {
		return ResultErrors(err)
	}

	events := e.collectEvents()
	if len(events) > 0 && e.options.EventPersist != nil {
		action, err := e.makeEventPersistAction(events)
		if err != nil {
			return ResultErrors(err)
		}
		if err := e.exec(ctx, []*Action{action}); err != nil {
			return ResultError(err)
		}
	}

	// 发送领域事件
	if len(events) > 0 {
		if e.eventBus == nil {
			return ResultErrors(ErrNoEventBusFound)
		}
		if err := e.dispatchEvents(ctx, events); err != nil {
			return ResultErrors(err)
		}
	}

	return e.result
}

type doSave func(ctx context.Context) *Result

func (e *Stage) runOnLock(f doSave, lockKeys ...string) doSave {
	return func(ctx context.Context) *Result {
		sort.Strings(lockKeys)
		for i := 1; i < len(lockKeys); i++ {
			if lockKeys[i] == lockKeys[i-1] {
				return ResultErrors(fmt.Errorf("lockKey(%s) repeated", lockKeys[i]))
			}
		}

		var lockErr error
		ls := make([]interface{}, 0)
		for _, id := range lockKeys {
			l, err := e.locker.Lock(ctx, fmt.Sprintf("ddd_engine_%s", id))
			if err != nil {
				lockErr = fmt.Errorf("acquiring redis locker failed: %v", err)
				break
			}
			ls = append(ls, l)
		}
		defer func() {
			for _, l := range ls {
				if err := e.locker.UnLock(ctx, l); err != nil {
					e.logger.Error(err, "unlock failed")
				}
			}
		}()

		if lockErr != nil {
			return ResultErrors(lockErr)
		}
		return f(ctx)
	}
}

func (e *Stage) runWithTransaction(f doSave) doSave {
	return func(ctx context.Context) *Result {
		ctx, err := e.executor.Begin(ctx)
		if err != nil {
			return ResultErrors(err)
		}
		defer func() {
			if r := recover(); r != nil {
				if err := e.executor.RollBack(ctx); err != nil {
					e.logger.Error(err, "rollback failed")
				}
				panic(r)
			}
		}()
		result := f(ctx)
		if result.Error != nil || result.Break {
			if err := e.executor.RollBack(ctx); err != nil {
				e.logger.Error(err, "rollback failed", "err", err)
			}
			// 回滚消息事务
			if e.eventBus != nil && e.eventCtx != nil {
				if txEventBus, ok := e.eventBus.(ITransactionEventBus); ok {
					_ = txEventBus.Rollback(e.eventCtx)
				}
			}
			return result
		}
		if err := e.executor.Commit(ctx); err != nil {
			result.Error = err
			e.logger.Error(err, "commit failed", "err", err)
		} else {
			// commit 成功后才提交消息事务
			// 注意 commit 失败不会执行 eventBus.Commit，需要依靠 eventbus 的回查机制确认最终结果
			if e.eventBus != nil && e.eventCtx != nil {
				if txEventBus, ok := e.eventBus.(ITransactionEventBus); ok {
					_ = txEventBus.Commit(e.eventCtx)
				}
			}
		}

		return result
	}
}

func (e *Stage) Save(ctx context.Context) *Result {
	do := e.do
	if e.options.WithTransaction {
		do = e.runWithTransaction(do)
	}

	if len(e.lockKeys) > 0 {
		do = e.runOnLock(do, e.lockKeys...)
	}

	res := do(ctx)
	if res.Error == nil && len(e.options.PostSaveHooks) > 0 {
		for _, h := range e.options.PostSaveHooks {
			h(ctx, res)
		}
	}
	return res
}
