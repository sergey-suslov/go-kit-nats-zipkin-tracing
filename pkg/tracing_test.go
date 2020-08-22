package pkg

import (
	"bytes"
	"context"
	"encoding/json"
	kitnats "github.com/go-kit/kit/transport/nats"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/propagation/b3"
	"testing"
)

func TestNATSSubscriberTraceSuccess(t *testing.T) {
	var (
		spanName = "test-span-name"
	)
	spans, testMessage, res := makeFakeNATSRequest(Name(spanName))

	if bytes.Compare(testMessage, res) != 0 {
		t.Fatal("Data was corrupted during the request")
	}

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

func TestNATSSubscriberTraceSuccessWithRegularMessage(t *testing.T) {
	var (
		spanName = "test-span-name"
	)
	spans, testMessage, res := makeFakeNATSRequestWithoutSpan(Name(spanName))

	if bytes.Compare(testMessage, res) != 0 {
		t.Fatal("Data was corrupted during the request")
	}

	if len(spans) != 1 {
		t.Fatalf("spans len equals to %d, t be 1", len(spans))
	}

	if spans[0].ParentID != nil {
		t.Fatalf("child span must have ParentId equals to nil, found %s", spans[1].ID)
	}
}

func TestNATSSubscriberTraceOptions(t *testing.T) {
	var (
		spanName = "test-span-name"
		testTags = map[string]string{"tag1": "value1", "tag2": "value2"}
	)

	spans, _, _ := makeFakeNATSRequest(Name(spanName), Tags(testTags))

	if len(spans[0].Tags) != len(testTags) {
		t.Fatalf("tags number differs, must be %d, found %d", len(testTags), len(spans[0].Tags))
	}

	for k, v := range spans[0].Tags {
		if testTags[k] != v {
			t.Fatalf("tags differs, must be %s, but found %s", testTags[k], v)
		}
	}
}

func makeFakeNATSRequest(opts ...TracerOption) ([]model.SpanModel, []byte, []byte) {
	span, rec, tr := createSpanAndRecorder()
	defer rec.Close()

	response := make(chan []byte, 1)

	handler := createTransparentHandler(NATSSubscriberTrace(tr, opts...), response)

	testMessageWithContext, testMessage := createTestNatsMessageData(span)
	msg := &nats.Msg{
		Data:  testMessageWithContext,
		Reply: "anywhere",
	}
	var nc *nats.Conn
	handler.ServeMsg(nc)(msg)

	res := <-response
	span.Finish()

	spans := rec.Flush()
	return spans, testMessage, res
}

func makeFakeNATSRequestWithoutSpan(opts ...TracerOption) ([]model.SpanModel, []byte, []byte) {
	tr, rec := createTracer()
	defer rec.Close()

	response := make(chan []byte, 1)

	handler := createTransparentHandler(NATSSubscriberTrace(tr, opts...), response)

	testMessage, _ := json.Marshal(testMessageData)
	msg := &nats.Msg{
		Data:  testMessage,
		Reply: "anywhere",
	}
	var nc *nats.Conn
	handler.ServeMsg(nc)(msg)

	res := <-response

	spans := rec.Flush()
	return spans, testMessage, res
}

func createTransparentHandler(o kitnats.SubscriberOption, response chan<- []byte) *kitnats.Subscriber {
	return kitnats.NewSubscriber(
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
		o,
	)
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
