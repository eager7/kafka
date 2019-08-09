/****************************************************************************
 * MODULE:             此模块用于创建kafka的消费群组以及写入对象，kafka的消费有两种模式，
 * 按照分组消费以及按照分区消费，两者只能选择一种使用，按照分组消费时，offset由kafka负责维护，
 * 按照分区消费则需要手动指定offset，否则从0开始，kafka有时需要配置host才能访问，注意配置项。
 * COMPONENT:          kafka package of reader and writer
 * REVISION:           $Revision:  1.0$
 * DATED:              $Date: 2019年 8月 9日 星期五 09时17分30秒 CST
 * AUTHOR:             PCT
 ****************************************************************************
 * Copyright panchangtao@gmail.com 2019. All rights reserved
 ***************************************************************************/
package kafka

import (
	"context"
	"errors"
	"github.com/eager7/elog"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

var logger = elog.NewLogger("kafka", elog.DebugLevel)

type Handler func(topic string, partition int, offset, lag int64, key, value []byte)
type Kafka struct {
	broker  []string
	group   string
	topic   string
	writer  *kafka.Writer
	readers map[int64]*kafka.Reader
}

/**
** 初始化时必须要指定broker和topic
** group以及parts只能选择其中之一进行指定，指定group时kafka自动维护offset，指定part时，offset默认从0开始，此时需要手动指定offset，开始接收后就会自动增长了
** parts参数示例为{part,offset}，如parts := [][2]int64{{0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}}
 */
func Initialize(broker []string, topic, group string, parts ...[2]int64) (*Kafka, error) {
	k := &Kafka{
		broker:  broker,
		group:   group,
		topic:   topic,
		readers: make(map[int64]*kafka.Reader),
	}
	if group != "" || len(parts) != 0 { //需要初始化读客户端
		//分区和分组只能设定一个
		if len(parts) > 0 {
			logger.Info("initialize kafka with parts:", parts)
			for _, p := range parts {
				if r, err := newReader(broker, topic, group, p[0], p[1]); err != nil {
					return nil, errors.New("kafka init err:" + err.Error())
				} else {
					k.readers[p[0]] = r
				}
			}
		} else {
			logger.Info("initialize kafka with group:", group)
			r, err := newReader(broker, topic, group, -1, -1)
			if err != nil {
				return nil, errors.New("kafka init err:" + err.Error())
			}
			k.readers[-1] = r
		}
	}
	k.writer = newWriter(broker, topic)
	return k, nil
}

func newWriter(broker []string, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      broker,
		Topic:        topic,
	})
}

func newReader(broker []string, topic, group string, part, offset int64) (*kafka.Reader, error) {
	cfg := kafka.ReaderConfig{
		Brokers:        broker,
		Topic:          topic,
		CommitInterval: time.Second, //不在每次取数据后commit游标，而是定期commit游标，可以提升性能
	}
	if part >= 0 {
		cfg.Partition = int(part)
	} else {
		cfg.GroupID = group
	}

	reader := kafka.NewReader(cfg)
	if offset >= 0 {
		if err := reader.SetOffset(offset); err != nil {
			return nil, errors.New("set offset err:" + err.Error())
		}
	}
	return reader, nil
}

func (k *Kafka) Routine(ctx context.Context, wg *sync.WaitGroup, handler Handler) {
	for part, reader := range k.readers { //每个reader启动一个线程处理，如果是按照分区启动，那么每个分区都会启动相应线程数
		wg.Add(1)
		logger.Debug("start reader part:", part, " offset:", reader.Offset())
		go readRoutine(ctx, wg, reader, handler)
	}
}

func readRoutine(ctx context.Context, wg *sync.WaitGroup, reader *kafka.Reader, handler Handler) {
	for {
		select {
		default:
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				logger.Error("kafka read message err:", err)
				wg.Done()
				_ = reader.Close()
				return
			}
			logger.Debug("kafka message:", m.Topic, m.Partition, m.Offset, reader.Lag())
			if handler != nil {
				handler(m.Topic, m.Partition, m.Offset, reader.Lag(), m.Key, m.Value)
			}
		case <-ctx.Done():
			logger.Warn("quit kafka routine")
			wg.Done()
			_ = reader.Close()
			return
		}
	}
}

func (k *Kafka) SendMessage(ctx context.Context, key, value []byte) error {
	if err := k.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value}); err != nil {
		return errors.New("kafka write message error:" + err.Error())
	}
	return nil
}