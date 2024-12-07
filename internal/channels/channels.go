package channels

import (
	"context"
	"fmt"
)

func PrefixStringChannel(ctx context.Context, prefix string, input <-chan string, output chan<- string) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			continue
		case msg, ok := <-input:
			if !ok {
				return
			}

			output <- fmt.Sprintf("%s %s", prefix, msg)
		}
	}
}

func WrapErrorChannel(ctx context.Context, wrap string, input <-chan error, output chan<- error) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			continue
		case err, ok := <-input:
			if !ok {
				return
			}

			output <- fmt.Errorf("%s %w", wrap, err)
		}
	}
}
