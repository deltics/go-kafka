package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type configMap map[string]interface{}

func (cm configMap) copy() configMap {
	copy := configMap{}
	for k, v := range cm {
		copy[k] = v
	}
	return copy
}

func (cm configMap) configMap() *kafka.ConfigMap {
	kcm := kafka.ConfigMap{}
	for k, v := range cm {
		kcm[k] = v
	}
	kcm["bla"] = "vv"
	return &kcm
}
