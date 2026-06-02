package stomp

import (
	"context"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/domain"
)

type SendRequest struct {
	Destination     string                 `json:"destination"`
	DestinationType domain.DestinationType `json:"destinationType"`
	ContentType     string                 `json:"contentType"`
	Body            string                 `json:"body"`
	Headers         map[string]string      `json:"headers"`
}

func SendTestMessage(ctx context.Context, cfg config.Config, request SendRequest) error {
	destinationType := request.DestinationType
	if destinationType == "" {
		destinationType = domain.DestinationQueue
	}
	contentType := request.ContentType
	if contentType == "" {
		contentType = "application/json"
	}

	conn, err := gostomp.Dial(
		"tcp",
		cfg.STOMPAddr,
		gostomp.ConnOpt.Login(cfg.STOMPUser, cfg.STOMPPassword),
		gostomp.ConnOpt.HeartBeat(5*time.Second, 5*time.Second),
	)
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	done := make(chan error, 1)
	go func() {
		done <- conn.Send(
			STOMPDestination(destinationType, request.Destination),
			contentType,
			[]byte(request.Body),
			withHeaders(request.Headers),
		)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func withHeaders(headers map[string]string) func(*frame.Frame) error {
	return func(f *frame.Frame) error {
		for key, value := range headers {
			f.Header.Set(key, value)
		}
		return nil
	}
}
