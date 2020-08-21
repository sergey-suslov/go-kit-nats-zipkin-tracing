package pkg

import (
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/propagation"
	"github.com/openzipkin/zipkin-go/propagation/b3"
)

var ErrEmptyContext = errors.New("empty request context")

type natsMessageWithContext struct {
	Sc   b3.Map `json:"sc"`
	Data []byte `json:"data"`
}

// ExtractNATS will extract a span.Context from a NATS message.
func ExtractNATS(msg *nats.Msg) propagation.Extractor {
	return func() (*model.SpanContext, error) {
		var payload natsMessageWithContext
		err := json.Unmarshal(msg.Data, &payload)
		if err != nil {
			return nil, nil
		}

		//msg.Data = payload.Data

		sc, err := payload.Sc.Extract()
		if err != nil {
			return nil, err
		}

		if (model.SpanContext{}) == *sc {
			return nil, ErrEmptyContext
		}

		if sc.TraceID.Empty() {
			return nil, ErrEmptyContext
		}

		return sc, nil
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

		mappedSC := make(b3.Map)
		err := mappedSC.Inject()(sc)
		if err != nil {
			return err
		}

		messageWithContext := natsMessageWithContext{
			Sc:   mappedSC,
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
