package pkg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	kitnats "github.com/go-kit/kit/transport/nats"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/propagation/b3"
	"strings"
	"testing"
	"time"
)

var natsServer *server.Server

func init() {
	natsServer, _ = server.NewServer(&server.Options{
		Host: "localhost",
		Port: 4222,
	})

	go func() {
		natsServer.Start()
	}()

	if ok := natsServer.ReadyForConnections(2 * time.Second); !ok {
		panic("Failed start of NATS")
	}
}

func newNatsConn(t *testing.T) *nats.Conn {
	// Subscriptions and connections are closed asynchronously, so it's possible
	// that there's still a subscription from an old connection that must be closed
	// before the current test can be run.
	for tries := 20; tries > 0; tries-- {
		if natsServer.NumSubscriptions() == 0 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	if n := natsServer.NumSubscriptions(); n > 0 {
		t.Fatalf("found %d active subscriptions on the server", n)
	}

	nc, err := nats.Connect("nats://"+natsServer.Addr().String(), nats.Name(t.Name()))
	if err != nil {
		t.Fatalf("failed to connect to gnatsd server: %s", err)
	}

	return nc
}

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

func TestNATSSubscriberTraceSuccessWithErrorChecker(t *testing.T) {
	var (
		spanName  = "test-span-name"
		testError = errors.New("test error")
	)
	spans, _, _ := makeFakeNATSRequestWithoutSpan(Name(spanName), ErrChecker(func(msg *nats.Msg) error {
		return testError
	}))
	if strings.Compare(spans[0].Tags[string(zipkin.TagError)], testError.Error()) != 0 {
		t.Fatalf("error tag must be presented")
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

	compareTags(t, testTags, spans[0].Tags)
}

func TestNATSPublisherTraceSuccess(t *testing.T) {
	var (
		testSubject = "nats.test"
		testTags    = map[string]string{"tag1": "value1", "tag2": "value2"}
	)
	tr, rec := createTracer()
	defer rec.Close()

	nc := newNatsConn(t)

	testMessage, _ := json.Marshal(testMessageData)
	sub, err := nc.QueueSubscribe(testSubject, "natstracing", func(msg *nats.Msg) {
		if err := nc.Publish(msg.Reply, testMessage); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	publisher := kitnats.NewPublisher(nc, testSubject, func(ctx context.Context, msg *nats.Msg, i interface{}) error {
		msg.Data = i.([]byte)
		return nil
	}, func(ctx context.Context, msg *nats.Msg) (response interface{}, err error) {
		return msg.Data, nil
	}, NATSPublisherTrace(tr, Tags(testTags)))

	res, _ := publisher.Endpoint()(context.Background(), testMessage)
	response, ok := res.([]byte)
	if !ok {
		t.Fatal("response should be []byte")
	}
	if bytes.Compare(response, testMessage) != 0 {
		t.Fatal("response message should be equal to request message")
	}

	spans := rec.Flush()
	if len(spans) != 1 {
		t.Fatalf("there must be only one span, found %d", len(spans))
	}
	if spans[0].Kind != model.Producer {
		t.Fatalf("the span must have Kind set to Consumer, found %s", spans[0].Kind)
	}
	compareTags(t, testTags, spans[0].Tags)
}

func compareTags(t *testing.T, tags1, tags2 map[string]string) {
	for k, v := range tags1 {
		if tags2[k] != v {
			t.Fatalf("tags differs, must be %s, but found %s", v, tags2[k])
		}
	}
}

func makeFakeNATSRequest(opts ...TracerOption) ([]model.SpanModel, []byte, []byte) {
	span, rec, tr := createSpanAndRecorder()
	defer rec.Close()

	response := make(chan []byte, 1)

	handler := createEmptyHandler(NATSSubscriberTrace(tr, opts...), response)

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

	handler := createEmptyHandler(NATSSubscriberTrace(tr, opts...), response)

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

func createEmptyHandler(o kitnats.SubscriberOption, response chan<- []byte) *kitnats.Subscriber {
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
