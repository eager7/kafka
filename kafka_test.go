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
	broker = "120.25.224.24:9092"
	topic  = "plainchant"
	group  = "g1"
)

func TestGroup(t *testing.T) {
	k, err := Initialize(strings.Split(broker, ","), topic, group, false, false)
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
