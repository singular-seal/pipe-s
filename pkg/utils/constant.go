package utils

import (
	"github.com/Shopify/sarama"
	"time"
)

var SaramaVersion = sarama.V3_3_1_0

const DefaultCheckProgressInterval = time.Second * 30
