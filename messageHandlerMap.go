package kafka

type messageHandlerMap map[string]MessageHandler

func (thm messageHandlerMap) copy() messageHandlerMap {
	copy := messageHandlerMap{}
	for k, v := range thm {
		copy[k] = v
	}
	return copy
}

func (thm messageHandlerMap) topicIds() []string {
	ids := make([]string, 0, len(thm))
	for k := range thm {
		ids = append(ids, k)
	}
	return ids
}
