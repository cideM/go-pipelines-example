package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"
)

// If any go routine errors, it should send the error on its own error channel
// and close its own output channel. All error channels need to be merged. The
// go routine that listens to the error channel, probably in a select where it
// also drains the value channel, will cancel the context in case an error
// happens.  The original go routine that sent the error won't see the context
// cancellation since it already stopped doing whatever it was doing.

func producer(ctx context.Context, strings []string) (<-chan string, error) {
	outChannel := make(chan string)

	go func() {
		defer close(outChannel)
		for _, s := range strings {

			// Fake a slow operation
			time.Sleep(time.Second * 1)

			select {
			case <-ctx.Done():
				close(outChannel)
				return
			default:
				outChannel <- s
			}
		}
	}()

	return outChannel, nil
}

func transformer(ctx context.Context, input <-chan string) (<-chan string, <-chan error, error) {
	outChannel := make(chan string)
	errorChannel := make(chan error)

	go func() {
		defer close(outChannel)
		defer close(errorChannel)

		for s := range input {
			select {
			case <-ctx.Done():
				close(outChannel)
				close(errorChannel)
				return
			default:
				if s == "bar" {
					errorChannel <- errors.New("oh no :(")
				} else {
					outChannel <- strings.ToUpper(s)
				}
			}
		}
	}()

	return outChannel, errorChannel, nil
}

func consumer(ctx context.Context, cancelFunc context.CancelFunc, values <-chan string, errors <-chan error) {
	for val := range values {
		select {
		case <-ctx.Done():
			return
		case err := <-errors:
			if err != nil {
				cancelFunc()
				log.Println(err.Error())
			}
		default:
			log.Println(val)
		}
	}
}

func main() {
	source := []string{"foo", "bar", "bax"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chan1, err := producer(ctx, source)
	if err != nil {
		log.Fatal(err)
	}

	chan2, errChan1, err := transformer(ctx, chan1)
	if err != nil {
		log.Fatal(err)
	}

	consumer(ctx, cancel, chan2, errChan1)
}
