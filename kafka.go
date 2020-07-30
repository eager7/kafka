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
	"fmt"
	"github.com/eager7/elog"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

var logger = elog.NewLogger("kafka", elog.NoticeLevel)

/*
** 处理回调，当发生错误时会传入错误信息err，应用层处理err并通过返回err来告知此读取协程是否需要退出(nil不退出，否则退出)，
** 当应用层在接收正常消息处理失败时也通过返回err来告知消息是否被消费完，返回nil表示消费完成，继续下一条消息
 */
type Handler func(topic string, partition int, offset, lag int64, key, value []byte, err error) (eResp error)
type Kafka struct {
	broker  []string                //kafka主机地址
	group   string                  //消费者组
	topic   string                  //消费主题
	writer  *kafka.Writer           //写数据接口
	Manual  bool                    //是否手动维护游标
	readers map[int64]*kafka.Reader //读数据接口
}

/**
** 初始化时必须要指定broker和topic
** group以及parts只能选择其中之一进行指定，指定group时kafka自动维护offset，指定part时，offset默认从0开始，此时需要手动指定offset，开始接收后就会自动增长了
** parts参数示例为{part,offset}，如parts := [][2]int64{{0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}}
** async定义写入时是同步写入还是异步写入，true为异步写入，忽略写入返回值，false为同步写入，等待写入返回，效率较低
 */
func Initialize(broker []string, topic, group string, async, manual bool, parts ...[2]int64) (*Kafka, error) {
	k := &Kafka{
		broker:  broker,
		group:   group,
		topic:   topic,
		readers: make(map[int64]*kafka.Reader),
		Manual:  manual,
	}
	if group != "" || len(parts) != 0 { //需要初始化读客户端
		//分区和分组只能设定一个
		if len(parts) > 0 {
			logger.Info("initialize kafka with parts:", parts)
			for _, p := range parts {
				if r, err := newReader(broker, topic, group, p[0], p[1], manual); err != nil {
					return nil, errors.New("kafka init err:" + err.Error())
				} else {
					k.readers[p[0]] = r
				}
			}
		} else {
			logger.Info("initialize kafka with group:", group)
			r, err := newReader(broker, topic, group, -1, -1, manual)
			if err != nil {
				return nil, errors.New("kafka init err:" + err.Error())
			}
			k.readers[-1] = r
		}
	}
	k.writer = newWriter(broker, topic, async)
	return k, nil
}

func newWriter(broker []string, topic string, async bool) *kafka.Writer {
	if async {
		return kafka.NewWriter(kafka.WriterConfig{
			Brokers: broker,
			Topic:   topic,
			Async:   true,
		})
	} else {
		return kafka.NewWriter(kafka.WriterConfig{
			Brokers:      broker,
			Topic:        topic,
			BatchTimeout: time.Microsecond * 10,
		})
	}
}

func newReader(broker []string, topic, group string, part, offset int64, manual bool) (*kafka.Reader, error) {
	cfg := kafka.ReaderConfig{
		Brokers: broker,
		Topic:   topic,
	}
	if !manual {
		cfg.CommitInterval = time.Second //不在每次取数据后commit游标，而是定期commit游标，可以提升性能，按分组消费时自动维护游标
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

/*
** 启动kafka消费者，worker是消费者数量，当按照主题消费时最好和分区数量一致，按照分区消费时设置为1即可
**/
func (k *Kafka) Start(ctx context.Context, wg *sync.WaitGroup, handler Handler, worker int) {
	for i := 0; i < worker; i++ {
		go k.routine(ctx, wg, handler)
	}
}

/*
** kafka消费者，同一个消费者组的消费者可以消费同一个topic的不同分区的数据，但是不会组内多个消费者消费同一分区的数据，最好将分区和消费者数量设置为相同大小
**/
func (k *Kafka) routine(ctx context.Context, wg *sync.WaitGroup, handler Handler) {
	for part, reader := range k.readers { //每个reader启动一个线程处理，如果是按照分区启动，那么每个分区都会启动相应线程数
		wg.Add(1)
		logger.Debug("start reader part:", part, " offset:", reader.Offset())
		go readRoutine(ctx, wg, reader, handler, k.Manual)
	}
}

func readRoutine(ctx context.Context, wg *sync.WaitGroup, reader *kafka.Reader, handler Handler, manual bool) {
	for {
		select {
		default:
			var err error
			var m kafka.Message
			if manual { //手动模式下只有应用层成功处理了消息才继续消费
				m, err = reader.FetchMessage(ctx)//测试发现，此模式下，kafka的current-offset没有增加，但是也不会重复消费，除非再次启动程序才会重新消费，和预期不符
			} else { //自动模式下由kafka维护offset
				m, err = reader.ReadMessage(ctx)
			}
			if err != nil {
				logger.Error("kafka read message err:", err)
				//返回错误表示应用层想要关闭此读取协程
				if handler != nil {
					if eResp := handler("", 0, 0, 0, nil, nil, err); eResp != nil {
						wg.Done()
						_ = reader.Close()
						return
					}
				}
				continue
			}
			fmt.Printf("kafka topic[%s], partition[%d], offset[%d], lag[%d], manual:[%v]-->key:%s,value:%s\n", m.Topic, m.Partition, m.Offset, reader.Lag(), manual, string(m.Key), string(m.Value))
			if handler != nil {
				//返回nil表示可以继续消费下一条，否则手动维护offset的模式下继续消费同一条
				if eResp := handler(m.Topic, m.Partition, m.Offset, reader.Lag(), m.Key, m.Value, nil); eResp == nil {
					if manual { //手动提交偏移量
						if err := reader.CommitMessages(ctx, m); err != nil {
							logger.Error("commit message failed:", err)
						}
					}
				}
			}
		case <-ctx.Done():
			logger.Warn("quit kafka routine")
			wg.Done()
			_ = reader.Close()
			return
		}
	}
}

/*
** 往kafka里写入数据，key是用来分区使用的，当使用同一个key，kafka会根据哈希算法放到同一个分区，如果不设置key，kafka自动分区
**/
func (k *Kafka) SendMessage(ctx context.Context, key, value []byte) error {
	if err := k.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value}); err != nil {
		return errors.New("kafka write message error:" + err.Error())
	}
	return nil
}
