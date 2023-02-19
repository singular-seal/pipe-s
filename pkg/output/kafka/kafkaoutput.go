package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"time"
)

const (
	DefaultBatchCount        = 10
	DefaultMaxBytes          = 10 * 1024 * 1024
	DefaultHeartbeatInterval = 30
	DefaultSendTimeout       = 30 * time.Second
)

type KafkaOutput struct {
	*core.BaseOutput
	config          *KafkaOutputConfig
	topicNames      map[string]bool
	saramaConfig    *sarama.Config
	client          sarama.Client
	producer        sarama.AsyncProducer
	heartbeatTicker *time.Ticker
	stopWaitContext context.Context
	stopCancel      context.CancelFunc
}

type KafkaOutputConfig struct {
	ID                    string
	KeyVariable           string // kafka partition key variable name
	TopicName             string // kafka topic name can be specified by TopicName or passed as a variable by
	TopicVariable         string // TopicVariable
	ServerAddresses       []string
	FlushBatch            int    // SaramaConfig.Producer.Flush.Messages and SaramaConfig.Producer.Flush.MaxMessages
	MaxMessageBytes       int    // SaramaConfig.Producer.MaxMessageBytes
	BlockWhenSizeTooLarge bool   // Whether block producing if message size too large error happens
	RequiredAcks          string // SaramaConfig.Producer.RequiredAcks
}

func NewKafkaOutput() *KafkaOutput {
	output := &KafkaOutput{
		BaseOutput: core.NewBaseOutput(),
	}
	return output
}

func (o *KafkaOutput) Start() (err error) {
	o.stopWaitContext, o.stopCancel = context.WithCancel(context.Background())
	if o.client, err = sarama.NewClient(o.config.ServerAddresses, o.saramaConfig); err != nil {
		return
	}
	if o.producer, err = sarama.NewAsyncProducerFromClient(o.client); err != nil {
		return
	}
	go o.handleAck()

	o.heartbeatTicker = time.NewTicker(time.Duration(DefaultHeartbeatInterval) * time.Second)
	go o.heartbeat()

	return
}

func (o *KafkaOutput) Stop() {
	o.stopCancel()
	if o.client != nil {
		if err := o.client.Close(); err != nil {
			o.GetLogger().Error("error close kafka client", log.Error(err))
		}
	}
}

func (o *KafkaOutput) Configure(config core.StringMap) (err error) {
	c := &KafkaOutputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	o.config = c
	o.saramaConfig = sarama.NewConfig()
	o.saramaConfig.Version = utils.SaramaVersion
	o.saramaConfig.ClientID = "pipes"
	o.saramaConfig.Metadata.Timeout = time.Second * 30
	o.saramaConfig.Producer.Return.Successes = true
	o.saramaConfig.Producer.Return.Errors = true
	o.saramaConfig.Producer.Retry.Max = 0 // disable retry to prevent disorder

	if len(c.TopicName) > 0 {
		o.topicNames = map[string]bool{c.TopicName: true}
	} else {
		o.topicNames = make(map[string]bool)
	}
	if len(c.KeyVariable) > 0 {
		o.saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner
	} else {
		o.saramaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	switch c.RequiredAcks {
	case "NoResponse":
		o.saramaConfig.Producer.RequiredAcks = sarama.NoResponse
	case "WaitForLocal":
		o.saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	case "WaitForAll":
		o.saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	default:
		o.saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	}

	//o.saramaConfig.Producer.Flush.Bytes = 1024 * 1024
	o.saramaConfig.Producer.Flush.Frequency = time.Millisecond

	if c.FlushBatch > 0 {
		o.saramaConfig.Producer.Flush.MaxMessages = c.FlushBatch
		o.saramaConfig.Producer.Flush.Messages = c.FlushBatch
	} else {
		o.saramaConfig.Producer.Flush.MaxMessages = DefaultBatchCount
		o.saramaConfig.Producer.Flush.Messages = DefaultBatchCount
	}

	if c.MaxMessageBytes > 0 {
		o.saramaConfig.Producer.MaxMessageBytes = c.MaxMessageBytes
	} else {
		o.saramaConfig.Producer.MaxMessageBytes = DefaultMaxBytes
	}
	return nil
}

func (o *KafkaOutput) Process(m *core.Message) {
	// find all topics for heart beat
	if len(o.config.TopicVariable) > 0 {
		if obj, ok := m.GetVariable(o.config.TopicVariable); ok {
			tn := obj.(string)
			if _, ok = o.topicNames[tn]; !ok {
				o.topicNames[tn] = true
			}
		}
	}

	km, err := o.createKafkaMessage(m)
	if err != nil {
		o.GetInput().Ack(m, err)
		return
	}

	if err = o.sendWithTimeout(km); err != nil {
		o.GetInput().Ack(m, err)
		return
	}
}

func (o *KafkaOutput) sendWithTimeout(message *sarama.ProducerMessage) error {
	select {
	case o.producer.Input() <- message:
	case <-time.After(DefaultSendTimeout):
		return errors.Errorf("KafkaOutput send timeout %s", message.Topic)
	}
	return nil
}

func (o *KafkaOutput) createKafkaMessage(m *core.Message) (*sarama.ProducerMessage, error) {
	data, ok := m.Body.([]byte)
	if !ok {
		return nil, errors.Errorf("KafkaOutput no bytes in msg, id %s", m.Header.ID)
	}
	var message *sarama.ProducerMessage
	var topicName string

	if len(o.config.TopicVariable) > 0 {
		if obj, ok := m.GetVariable(o.config.TopicVariable); ok {
			topicName = obj.(string)
		} else {
			return nil, errors.Errorf("KafkaOutput can't get TopicVariable, id %s", m.Header.ID)
		}
	} else {
		topicName = o.config.TopicName
	}

	message = &sarama.ProducerMessage{
		Topic:    topicName,
		Value:    sarama.ByteEncoder(data),
		Metadata: m,
	}

	if len(o.config.KeyVariable) > 0 {
		if k, ok := m.GetVariable(o.config.KeyVariable); ok {
			message.Key = sarama.ByteEncoder(fmt.Sprint(k))
		} else {
			return nil, errors.Errorf("key var not found, msg id %s", m.Header.ID)
		}
	}
	return message, nil
}

func (o *KafkaOutput) encodeBinary(value interface{}) ([]byte, error) {
	stringVal, ok1 := value.(string)
	if ok1 {
		return ([]byte)(stringVal), nil
	}
	bytesVal, ok2 := value.([]byte)
	if ok2 {
		return bytesVal, nil
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (o *KafkaOutput) handleAck() {
	for {
		select {
		case msg, ok := <-o.producer.Successes():
			if !ok {
				o.RaiseError(errors.Errorf("KafkaOutput fail get producer successes"))
				return
			}
			orgMsg, ok := msg.Metadata.(*core.Message)
			if !ok {
				o.RaiseError(errors.Errorf("KafkaOutput invalid metadata, key %s, value %s", msg.Key, msg.Value))
				return
			}
			o.GetInput().Ack(orgMsg, nil)
		case prodErr, ok := <-o.producer.Errors():
			if !ok {
				o.RaiseError(errors.Errorf("KafkaOutput fail get producer errors"))
				return
			}
			orgMsg, ok := prodErr.Msg.Metadata.(*core.Message)
			if !ok {
				o.RaiseError(errors.Errorf("KafkaOutput invalid metadata, key %s, value %s", prodErr.Msg.Key,
					prodErr.Msg.Value))
				return
			} else if !o.config.BlockWhenSizeTooLarge && prodErr.Err == sarama.ErrMessageSizeTooLarge {
				o.GetLogger().Info("KafkaOutput msg too large", log.String("id", orgMsg.Header.ID),
					log.Int("size", prodErr.Msg.Value.Length()))
				o.GetInput().Ack(orgMsg, nil)
				return
			} else {
				o.GetInput().Ack(orgMsg, prodErr)
				return
			}
		case <-o.stopWaitContext.Done():
			return
		}
	}
}

// heartbeat is to fix the sarama issue that Client will timeout if there's no data during the period
func (o *KafkaOutput) heartbeat() {
	for {
		select {
		case <-o.heartbeatTicker.C:
			if err := o.checkBrokers(); err != nil {
				o.GetLogger().Warn("fail check brokers", log.Error(err))
			}
		case <-o.stopWaitContext.Done():
			return
		}
	}
}

func (o *KafkaOutput) checkBrokers() (err error) {
	if o.client.Closed() {
		return sarama.ErrClosedClient
	}
	brokers := o.client.Brokers()
	tns := make([]string, 0)
	for k, _ := range o.topicNames {
		tns = append(tns, k)
	}
	for _, broker := range brokers {
		req := &sarama.MetadataRequest{Topics: tns}
		req.Version = 5
		_, err = broker.GetMetadata(req)
	}
	return
}
