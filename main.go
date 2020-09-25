package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/guoyk93/conc"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const BeanMillTestTube = "beanmill-test-tube"

func exit(err *error) {
	if *err != nil {
		log.Println("exited with error:", (*err).Error())
		os.Exit(1)
	} else {
		log.Println("exited")
	}
}

func main() {
	var err error
	defer exit(&err)

	addr := strings.TrimSpace(os.Getenv("BEANSTALK_ADDR"))
	if addr == "" {
		err = errors.New("missing environment: BEANSTALK_ADDR")
		return
	}

	chErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	chSig := make(chan os.Signal, 1)
	signal.Notify(chSig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		chErr <- conc.Parallel(routineSend(addr), routineRecv(addr)).Do(ctx)
	}()

	select {
	case err = <-chErr:
	case sig := <-chSig:
		log.Println("signal caught:", sig.String())
		cancel()
		<-chErr
	}
}

func routineSendSingle(ctx context.Context, addr string) (err error) {
	var c *beanstalk.Conn
	if c, err = beanstalk.Dial("tcp", addr); err != nil {
		return
	}
	defer c.Close()

	log.Println("SEND: connected")

	t := beanstalk.NewTube(c, BeanMillTestTube)

	var (
		bufRaw = make([]byte, 64, 64)
		bufHex = make([]byte, 128, 128)
	)

	for {
		// build body
		if _, err = rand.Read(bufRaw); err != nil {
			return
		}
		hex.Encode(bufHex, bufRaw)
		// send
		var id uint64
		if id, err = t.Put(bufHex, 1, 0, time.Second*10); err != nil {
			return
		}
		log.Printf("SEND: 0x%016x", id)
		// loop or wait done
		select {
		case <-time.After(time.Millisecond * 300):
		case <-ctx.Done():
			return
		}
	}
}

func routineSend(addr string) conc.TaskFunc {
	return func(ctx context.Context) error {
		for {
			if err := routineSendSingle(ctx, addr); err != nil {
				log.Println("SEND: error =", err.Error())
			}
			// loop or wait done
			select {
			case <-time.After(time.Second * 5):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func routineRecvSingle(ctx context.Context, addr string) (err error) {
	var c *beanstalk.Conn
	if c, err = beanstalk.Dial("tcp", addr); err != nil {
		return
	}
	defer c.Close()

	t := beanstalk.NewTubeSet(c, BeanMillTestTube)

	log.Println("RECV: connected")

	for {
		if ctx.Err() != nil {
			return
		}
		var id uint64
		if id, _, err = t.Reserve(time.Second * 5); err != nil {
			if err == beanstalk.ErrTimeout {
				err = nil
				continue
			}
			return
		}
		log.Printf("RECV: 0x%016x", id)
		_ = c.Delete(id)
	}
}

func routineRecv(addr string) conc.TaskFunc {
	return func(ctx context.Context) error {
		for {
			if err := routineRecvSingle(ctx, addr); err != nil {
				log.Println("RECV: error =", err.Error())
			}
			// loop or wait done
			select {
			case <-time.After(time.Second * 5):
			case <-ctx.Done():
				return nil
			}
		}
	}
}
