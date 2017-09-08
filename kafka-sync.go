package kafkasync

import (
	"crypto/sha256"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

type Syncer struct {
	// The topic to synchronize.
	Topic string

	// The topic's partition to synchronize.
	Partition int32

	// The value to use when a key is removed.
	RemovedValue []byte
}

func New(topic string) Syncer {
	return Syncer{
		Topic:        topic,
		Partition:    0,
		RemovedValue: []byte{},
	}
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type hash = [sha256.Size]byte

// kvSource mustn't send duplicate keys!
func (s Syncer) Sync(kafka sarama.Client, kvSource <-chan KeyValue) (stats *Stats, err error) {
	stats = &Stats{}

	topicHashes := map[hash]hash{}
	keyHashToValue := map[hash][]byte{}
	removedHash := sha256.Sum256(s.RemovedValue)

	startTime := time.Now()

	// Read the topic
	consumer, err := sarama.NewConsumerFromClient(kafka)
	if err != nil {
		return
	}

	pc, err := consumer.ConsumePartition(s.Topic, s.Partition, sarama.OffsetOldest)
	if err != nil {
		return
	}

	highWater, err := kafka.GetOffset(s.Topic, s.Partition, sarama.OffsetNewest)

	glog.Info("Reading topic ", s.Topic, ", partition ", s.Partition)
	if highWater > 0 {
		for m := range pc.Messages() {
			hw := pc.HighWaterMarkOffset()
			if hw > highWater {
				highWater = hw
			}
			glog.V(4).Info("-> offset: ", m.Offset, " / ", highWater-1)

			keyHash := sha256.Sum256(m.Key)
			valueHash := sha256.Sum256(m.Value)

			keyHashToValue[keyHash] = m.Key

			if valueHash == removedHash {
				delete(topicHashes, keyHash)
			} else {
				topicHashes[keyHash] = valueHash
			}

			if m.Offset+1 >= highWater {
				break
			}
		}
	}
	pc.Close()

	stats.ReadTopicDuration = time.Since(startTime)

	// Prepare producer
	producer, err := sarama.NewAsyncProducerFromClient(kafka)
	if err != nil {
		return
	}

	wg := &sync.WaitGroup{}
	if kafka.Config().Producer.Return.Errors {
		wg.Add(1)
		go func() {
			for prodError := range producer.Errors() {
				glog.Error(prodError)
				stats.ErrorCount += 1
			}
			wg.Done()
		}()
	} else {
		stats.ErrorCount = -1
	}

	if kafka.Config().Producer.Return.Successes {
		wg.Add(1)
		go func() {
			for range producer.Successes() {
				stats.SuccessCount += 1
			}
			wg.Done()
		}()
	} else {
		stats.SuccessCount = -1
	}

	producerInput := producer.Input()

	send := func(kv KeyValue) {
		producerInput <- &sarama.ProducerMessage{
			Topic:     s.Topic,
			Partition: s.Partition,
			Key:       sarama.ByteEncoder(kv.Key),
			Value:     sarama.ByteEncoder(kv.Value),
		}
		stats.SendCount += 1
	}

	// Compare and send changes
	startSyncTime := time.Now()

	for kv := range kvSource {
		kv := kv

		stats.Count += 1

		keyHash := sha256.Sum256(kv.Key)
		valueHash := sha256.Sum256(kv.Value)

		if currentHash, ok := topicHashes[keyHash]; ok {
			// seen hash (kvSource mustn't send duplicate keys!)
			delete(topicHashes, keyHash)

			if currentHash == valueHash {
				stats.Unchanged += 1
				continue
			}

			send(kv)
			stats.Modified += 1

		} else {
			send(kv)
			stats.Created += 1
		}
	}

	for topicKeyHash, _ := range topicHashes {
		topicKey := keyHashToValue[topicKeyHash]

		send(KeyValue{topicKey, s.RemovedValue})
		stats.Deleted += 1
	}

	producer.AsyncClose()
	wg.Wait()
	stats.SyncDuration = time.Since(startSyncTime)

	stats.TotalDuration = time.Since(startTime)

	return
}
