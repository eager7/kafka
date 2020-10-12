package kafka

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	broker = ""
	topic  = "topic_approve"
	group  = "g1"
)

func TestGroup(t *testing.T) {
	k, err := Initialize(strings.Split(broker, ","), topic, group, false, false)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	wg := &sync.WaitGroup{}

	k.Start(ctx, wg, ApproveHandle, 1)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	<-interrupt
	fmt.Println("stop kafka test")
	wg.Wait()
}

func ApproveHandle(topic string, partition int, offset, lag int64, key, value []byte, err error) (eResp error) {
	if err != nil {
		logger.Error("ReceivePushMessage err:", topic, "partition:", partition, "offset:", offset, "lag:", lag, "key:", key, "value:", string(value), err)
		return nil //不退出接收协程
	}
	logger.Debug("receive approve, topic:", topic, "partition:", partition, "offset:", offset, "lag:", lag, "key:", key, "value:", string(value))
	return nil //提交偏移量，消费下一条消息
}

func TestParts(t *testing.T) {
	parts := [][2]int64{{0, 0}}
	k, err := Initialize(strings.Split(broker, ","), topic, "", false, false, parts...)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	wg := &sync.WaitGroup{}
	k.Start(ctx, wg, nil, 1)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	<-interrupt
	fmt.Println("stop kafka test")
	wg.Wait()
}

func TestWrite(t *testing.T) {
	k, err := Initialize(strings.Split(broker, ","), topic, group, false, false)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		start := time.Now()
		if err := k.SendMessage(context.Background(), []byte("key"), []byte(fmt.Sprintf("test message:%d", i))); err != nil {
			t.Fatal(err)
		}
		fmt.Println("end:", i, time.Since(start))
	}
	time.Sleep(time.Second)
}
