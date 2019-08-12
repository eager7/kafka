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
)

const (
	broker = ""
	topic  = ""
	group  = ""
)

func TestGroup(t *testing.T) {
	k, err := Initialize(strings.Split(broker, ","), topic, group)
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
	parts := [][2]int64{{0, 100}, {1, 100}, {2, 100}, {3, 100}, {4, 100}, {5, 100}}
	k, err := Initialize(strings.Split(broker, ","), topic, "", parts...)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	wg := &sync.WaitGroup{}
	k.Start(ctx, wg, nil, 6)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	<-interrupt
	fmt.Println("stop kafka test")
	wg.Wait()
}
