package pkg

import (
	"bytes"
	"context"
	"encoding/json"
	kitnats "github.com/go-kit/kit/transport/nats"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/propagation/b3"
	"testing"
)

func TestNATSSubscriberTraceSuccess(t *testing.T) {
	var (
		spanName = "test-span-name"
	)
	span, rec, tr := createSpanAndRecorder()

	defer rec.Close()
	testMessageWithContext, testMessage := createTestNatsMessageData(span)

	response := make(chan []byte, 1)

	handler := kitnats.NewSubscriber(
		func(ctx context.Context, request interface{}) (response interface{}, err error) {
			return request, nil
		},
		func(ctx context.Context, msg *nats.Msg) (interface{}, error) {
			return msg.Data, nil
		},
		func(ctx context.Context, s string, conn *nats.Conn, i interface{}) error {
			response <- i.([]byte)
			return nil
		},
		NATSSubscriberTrace(tr, Name(spanName)),
	)

	var nc *nats.Conn
	msg := &nats.Msg{
		Data:  testMessageWithContext,
		Reply: "anywhere",
	}
	handler.ServeMsg(nc)(msg)

	res := <-response
	span.Finish()
	if bytes.Compare(testMessage, res) != 0 {
		t.Fatal("Data was corrupted during the request")
	}

	spans := rec.Flush()
	if len(spans) != 2 {
		t.Fatalf("spans len equals to %d, t be 2", len(spans))
	}

	if spans[0].ParentID == nil {
		t.Fatalf("child span must have ParentId equals to %s, found nil", spans[1].ID)
	}

	if *spans[0].ParentID != spans[1].ID {
		t.Fatalf("Child span must have ParentId equal %s, but found %s", spans[1].ID, *spans[0].ParentID)
	}
}

func createTestNatsMessageData(span zipkin.Span) (msgWithContext []byte, originalMsg []byte) {
	mappedSc := make(b3.Map)
	sc := span.Context()
	_ = mappedSc.Inject()(sc)
	marshalledTestMessageData, _ := json.Marshal(testMessageData)
	marshalledMessage, _ := json.Marshal(&natsMessageWithContext{
		Sc:   mappedSc,
		Data: marshalledTestMessageData,
	})
	return marshalledMessage, marshalledTestMessageData
}
