package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"sync"
	"time"
)

type KafkaInputConfig struct {
	ServerAddresses []string
	Topics          []string
	ConsumerGroup   string
	FromBeginning   bool
}

type KafkaPosition struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
}

type KafkaInput struct {
	*core.BaseInput
	config          *KafkaInputConfig
	saramaConfig    *sarama.Config
	kafkaClient     sarama.ConsumerGroup
	cancelFunction  context.CancelFunc
	lastAckPosition *KafkaPosition
	// sarama consumes multiple partitions by multiple goroutine, so need a lock here, better solution should be
	// modifying go disruptor to support multi producers mode
	sendLock sync.Mutex
}

func NewKafkaInput() *KafkaInput {
	in := &KafkaInput{
		BaseInput: core.NewBaseInput(),
	}
	return in
}

func (in *KafkaInput) Configure(config core.StringMap) (err error) {
	c := &KafkaInputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	in.config = c

	in.saramaConfig = sarama.NewConfig()
	in.saramaConfig.Version = utils.SaramaVersion
	in.saramaConfig.ClientID = "pipes"
	in.saramaConfig.Metadata.Timeout = time.Second * 30

	if in.config.FromBeginning {
		in.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	return
}

func (in *KafkaInput) Start() (err error) {
	if in.kafkaClient, err = sarama.NewConsumerGroup(in.config.ServerAddresses, in.config.ConsumerGroup, in.saramaConfig); err != nil {
		return
	}

	mc := MessageConsumer{
		ready: make(chan bool),
		input: in,
	}
	ctx, cancel := context.WithCancel(context.Background())
	in.cancelFunction = cancel
	go func() {
		for {
			if err = in.kafkaClient.Consume(ctx, in.config.Topics, &mc); err != nil {
				in.GetLogger().Error("KafkaInput connect fail", log.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			mc.ready = make(chan bool)
		}
	}()
	// wait till the consumer has been set up
	<-mc.ready
	in.GetLogger().Info("KafkaInput started")
	return
}

func (in *KafkaInput) Stop() {
	if in.cancelFunction != nil {
		in.cancelFunction()
	}
	in.kafkaClient.Close()
}

func (in *KafkaInput) Ack(msg *core.Message, err error) {
	if err != nil {
		in.RaiseError(err)
		return
	}
	session, ok := msg.GetMeta(core.MetaKafkaConsumerSession)
	if !ok {
		in.RaiseError(errors.Errorf("no session meta in msg %s", msg.Header.ID))
		return
	}
	obj, ok := msg.GetMeta(core.MetaKafkaConsumerPosition)
	if !ok {
		in.RaiseError(errors.Errorf("no position meta in msg %s", msg.Header.ID))
		return
	}

	position := obj.(*KafkaPosition)
	session.(sarama.ConsumerGroupSession).MarkOffset(position.Topic, position.Partition, position.Offset+1, "")
	in.lastAckPosition = position
}

func (in *KafkaInput) SetState(state []byte) error {
	return nil
}

func (in *KafkaInput) GetState() ([]byte, bool) {
	if in.lastAckPosition == nil {
		return nil, false
	}
	state, err := json.Marshal(in.lastAckPosition)
	return state, err != nil
}

type MessageConsumer struct {
	ready chan bool
	input *KafkaInput
}

func (mc *MessageConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(mc.ready)
	return nil
}

func (mc *MessageConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (mc *MessageConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	for msg := range claim.Messages() {
		m := core.NewMessage(core.TypeBytes)
		m.Header.ID = fmt.Sprintf("%s.%d.%d", msg.Topic, msg.Partition, msg.Offset)
		m.Header.CreateTime = uint64(time.Now().UnixNano())
		m.Data = msg.Value

		pos := &KafkaPosition{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp.Unix(),
		}
		m.SetMeta(core.MetaKafkaConsumerPosition, pos)
		m.SetMeta(core.MetaKafkaConsumerSession, session)
		// multiple goroutines can be used to consume, so a lock is required. Should let go disruptor support
		// multiple producer mode in the future.
		mc.input.sendLock.Lock()
		mc.input.GetOutput().Process(m)
		mc.input.sendLock.Unlock()
	}
	return
}
