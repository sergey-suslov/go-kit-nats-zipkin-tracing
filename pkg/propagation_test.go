package pkg

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/propagation/b3"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
	"testing"
)

type testMessageModel struct {
	Name string
	Id   int32
}

var (
	spanName        = "test-span"
	testMessageData = testMessageModel{
		Name: "bob",
		Id:   1,
	}
)

func TestInjectNATSRootSpan(t *testing.T) {
	span, rec, tr := createSpanAndRecorder()
	childSpan, _ := tr.StartSpanFromContext(zipkin.NewContext(context.Background(), span), spanName)
	defer rec.Close()

	t.Run("RootSpan", func(t *testing.T) {
		InjectNATSSpanTest(t, span)
	})

	t.Run("ChildSpan", func(t *testing.T) {
		InjectNATSSpanTest(t, childSpan)
	})
}

func InjectNATSSpanTest(t *testing.T, span zipkin.Span) {
	marshalledMessage, _ := json.Marshal(testMessageData)
	msg := nats.Msg{Data: marshalledMessage}

	sc := span.Context()
	_ = InjectNATS(&msg)(sc)

	var payload natsMessageWithContext
	_ = json.Unmarshal(msg.Data, &payload)

	if bytes.Compare(marshalledMessage, payload.Data) != 0 {
		t.Fatal("Message data is corrupted")
	}

	extractedSc, _ := payload.Sc.Extract()

	compareSpanContexts(t, extractedSc, &sc)
}

func TestExtractNATSRootSpan(t *testing.T) {
	span, rec, tr := createSpanAndRecorder()
	childSpan, _ := tr.StartSpanFromContext(zipkin.NewContext(context.Background(), span), spanName)
	defer rec.Close()

	t.Run("RootSpan", func(t *testing.T) {
		ExtractNATSSpanTest(t, span)
	})

	t.Run("ChildSpan", func(t *testing.T) {
		ExtractNATSSpanTest(t, childSpan)
	})
}

func ExtractNATSSpanTest(t *testing.T, span zipkin.Span) {
	mappedSc := make(b3.Map)
	sc := span.Context()
	_ = mappedSc.Inject()(sc)
	marshalledTestMessageData, _ := json.Marshal(testMessageData)
	marshalledMessage, _ := json.Marshal(&natsMessageWithContext{
		Sc:   mappedSc,
		Data: marshalledTestMessageData,
	})

	msg := &nats.Msg{
		Data: marshalledMessage,
	}

	extractedSc, _ := ExtractNATS(msg)()

	if bytes.Compare(marshalledTestMessageData, msg.Data) != 0 {
		t.Fatal("Message data is corrupted")
	}

	if extractedSc == nil {
		t.Fatalf("Extracted context is nil")
	}

	compareSpanContexts(t, extractedSc, &sc)
}

func ExtractNATSSpanRegularMessageTest(t *testing.T, span zipkin.Span) {
	mappedSc := make(b3.Map)
	sc := span.Context()
	_ = mappedSc.Inject()(sc)
	marshalledTestMessageData, _ := json.Marshal(testMessageData)

	msg := &nats.Msg{
		Data: marshalledTestMessageData,
	}

	extractedSc, _ := ExtractNATS(msg)()

	if bytes.Compare(marshalledTestMessageData, msg.Data) != 0 {
		t.Fatal("Message data is corrupted")
	}

	if extractedSc == nil {
		t.Fatalf("Extracted context is nil")
	}

	compareSpanContexts(t, extractedSc, &sc)
}

func createTracer() (*zipkin.Tracer, *recorder.ReporterRecorder) {
	rec := recorder.NewReporter()
	tr, _ := zipkin.NewTracer(
		rec,
		zipkin.WithSharedSpans(false),
	)
	return tr, rec
}

func createSpanAndRecorder() (zipkin.Span, *recorder.ReporterRecorder, *zipkin.Tracer) {
	tr, rec := createTracer()
	span := tr.StartSpan(spanName)
	return span, rec, tr
}

func compareSpanContexts(t *testing.T, sc1, sc2 *model.SpanContext) {
	if sc1.ID != sc2.ID {
		t.Fatalf("Injacted span context is not equal to extracted with b3 one: %s is diffrent", "ID")
	}
	if sc1.TraceID != sc2.TraceID {
		t.Fatalf("Injacted span context is not equal to extracted with b3 one: %s is diffrent", "TraceID")
	}
	if sc1.Err != sc2.Err {
		t.Fatalf("Injacted span context is not equal to extracted with b3 one: %s is diffrent", "Err")
	}
	if sc1.Debug != sc2.Debug {
		t.Fatalf("Injacted span context is not equal to extracted with b3 one: %s is diffrent", "Debug")
	}
	if sc1.ParentID != nil && sc2.ParentID != nil && *sc1.ParentID != *sc2.ParentID {
		t.Fatalf("Injacted span context is not equal to extracted with b3 one: %s is diffrent", "ParentID")
	}
	if *sc1.Sampled != *sc2.Sampled {
		t.Fatalf("Injacted span context is not equal to extracted with b3 one: %s is diffrent", "Sampled")
	}
}
