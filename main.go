package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
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
			select {
			case <-ctx.Done():
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

			// Fake a slow operation
			time.Sleep(time.Second * 1)

			select {
			case <-ctx.Done():
				return
			default:
				// outChannel <- strings.ToUpper(s)
				if s == "bar" {
					errorChannel <- errors.New("oh no :(")
					return
				} else {
					outChannel <- strings.ToUpper(s)
				}
			}
		}
	}()

	return outChannel, errorChannel, nil
}

func sink(ctx context.Context, cancelFunc context.CancelFunc, values <-chan string, errors <-chan error) {
	for {
		select {
		case <-ctx.Done():
			log.Print(ctx.Err().Error())
			return
		case err := <-errors:
			if err != nil {
				log.Println("error: ", err.Error())
				cancelFunc()
				return
			}
		case val, ok := <-values:
			if ok {
				log.Println("val: ", val)
			} else {
				log.Println("no more values")
				return
			}
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

	stringChans := []<-chan string{}
	errorChans := []<-chan error{}

	for i := 0; i < 3; i++ {
		strChan, errChan, err := transformer(ctx, chan1)
		if err != nil {
			log.Fatal(err)
		}

		stringChans = append(stringChans, strChan)
		errorChans = append(errorChans, errChan)
	}

	outChan := mergeStringChans(ctx, stringChans...)
	errChan := mergeErrorChans(ctx, errorChans...)

	sink(ctx, cancel, outChan, errChan)
}

func mergeStringChans(ctx context.Context, cs ...<-chan string) <-chan string {
	var wg sync.WaitGroup
	out := make(chan string)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan string) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func mergeErrorChans(ctx context.Context, cs ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	out := make(chan error)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan error) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
