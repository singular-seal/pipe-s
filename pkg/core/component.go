package core

import "github.com/singular-seal/pipe-s/pkg/log"

// Component is an entity which has id and lifecycle and can be configured.
type Component interface {
	Configurable
	LifeCycle
}

// Pipeline is a component receives data from an input and sends it to an output, and during the process
// the data maybe processed by several Processors.
type Pipeline interface {
	Input
	Output
	SetProcessors(processors []Processor)
}

type Processor interface {
	Component
	Process(msg *Message) (skip bool, err error)
}

type Input interface {
	Component
	Errorable
	SetOutput(output Output)
	Ack(msg *Message, err error)
	SetState(state []byte) error
	GetState() ([]byte, bool)
}

type Output interface {
	Component
	SetInput(input Input)
	Process(msg *Message)
}

type Errorable interface {
	Errors() chan error
}

// LogAware is used to inject logger to other components
type LogAware interface {
	SetLogger(logger *log.Logger)
}

type BaseComponent struct {
	ID     string
	logger *log.Logger
}

func (c *BaseComponent) Configure(config StringMap) error {
	if id, ok := config["ID"]; ok {
		if sid, ok := id.(string); ok {
			c.ID = sid
		}
	}
	return nil
}

func (c *BaseComponent) GetID() string {
	return c.ID
}

func (c *BaseComponent) Start() error {
	return nil
}

func (c *BaseComponent) Stop() {
}

func (c *BaseComponent) SetLogger(logger *log.Logger) {
	c.logger = logger
}

func (c *BaseComponent) GetLogger() *log.Logger {
	return c.logger
}

func NewBaseComponent() *BaseComponent {
	return &BaseComponent{}
}

type BaseInput struct {
	*BaseComponent
	output    Output
	errorChan chan error
}

func NewBaseInput() *BaseInput {
	return &BaseInput{
		BaseComponent: &BaseComponent{},
		errorChan:     make(chan error, 1),
	}
}

func (in *BaseInput) RaiseError(err error) {
	if len(in.errorChan) > 0 {
		return
	}
	in.errorChan <- err
}

func (in *BaseInput) SetOutput(output Output) {
	in.output = output
}

func (in *BaseInput) GetOutput() Output {
	return in.output
}

func (in *BaseInput) Errors() chan error {
	return in.errorChan
}

type BaseOutput struct {
	*BaseComponent
	input Input
}

func (out *BaseOutput) SetInput(input Input) {
	out.input = input
}

func (out *BaseOutput) GetInput() Input {
	return out.input
}

func NewBaseOutput() *BaseOutput {
	return &BaseOutput{
		BaseComponent: &BaseComponent{},
	}
}

// ComponentBuilder is the factory of Components
type ComponentBuilder interface {
	// RegisterComponent after registering builder is able to create component of
	RegisterComponent(typeName string, constructor ComponentConstructor)
	CreatePipeline(config StringMap) (pipe Pipeline, err error)
	CreateProcessor(config StringMap) (processor Processor, err error)
}

// ComponentConstructor represents a Component's constructor.
type ComponentConstructor func() Component

var componentBuilderInstance ComponentBuilder

func GetComponentBuilderInstance() ComponentBuilder {
	return componentBuilderInstance
}

func SetComponentBuilderInstance(b ComponentBuilder) {
	componentBuilderInstance = b
}
