package pkg

import (
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/propagation"
)

var ErrEmptyContext = errors.New("empty request context")

type natsMessageWithContextOnExtraction struct {
	Sc   model.SpanContext `json:"sc"`
	Data []byte            `json:"data"`
}

type natsMessageWithContextOnInjection struct {
	Sc   model.SpanContext `json:"sc"`
	Data interface{}       `json:"data"`
}

// ExtractNATS will extract a span.Context from a NATS message.
func ExtractNATS(msg *nats.Msg) propagation.Extractor {
	return func() (*model.SpanContext, error) {
		var payload natsMessageWithContextOnExtraction
		err := json.Unmarshal(msg.Data, &payload)
		// not natsMessageWithContext
		if err != nil {
			return nil, nil
		}

		msg.Data = payload.Data

		if (model.SpanContext{}) == payload.Sc {
			return nil, ErrEmptyContext
		}

		if payload.Sc.TraceID.Empty() {
			return nil, ErrEmptyContext
		}

		return &payload.Sc, nil
	}
}

// InjectNATS will inject a span.Context into NATS message.
func InjectNATS(msg *nats.Msg) propagation.Injector {
	return func(sc model.SpanContext) error {
		if (model.SpanContext{}) == sc {
			return ErrEmptyContext
		}

		if sc.TraceID.Empty() || sc.ID == 0 {
			return nil
		}
		messageWithContext := natsMessageWithContextOnInjection{
			Sc:   sc,
			Data: msg.Data,
		}
		marshalledMessage, err := json.Marshal(&messageWithContext)
		if err != nil {
			return err
		}
		msg.Data = marshalledMessage

		return nil
	}
}
