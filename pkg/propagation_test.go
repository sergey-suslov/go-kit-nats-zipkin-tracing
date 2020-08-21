package pkg

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
	"testing"
)

var (
	spanName        = "test-span"
	testMessageData = struct {
		Name string
		Id   int32
	}{
		Name: "bob",
		Id:   1,
	}
)

func TestInjectNATSRootSpan(t *testing.T) {
	span, rec, _ := createSpanAndRecorder()
	defer rec.Close()

	marshaledMessage, _ := json.Marshal(testMessageData)
	msg := nats.Msg{Data: marshaledMessage}

	sc := span.Context()
	_ = InjectNATS(&msg)(sc)

	var payload natsMessageWithContextOnInjection
	_ = json.Unmarshal(msg.Data, &payload)

	extractedSc, _ := payload.Sc.Extract()

	compareSpanContexts(t, extractedSc, &sc)
}

func TestInjectNATSChildSpan(t *testing.T) {
	parentSpan, rec, tr := createSpanAndRecorder()
	span, _ := tr.StartSpanFromContext(zipkin.NewContext(context.Background(), parentSpan), spanName)
	defer rec.Close()

	marshaledMessage, _ := json.Marshal(testMessageData)
	msg := nats.Msg{Data: marshaledMessage}

	sc := span.Context()
	_ = InjectNATS(&msg)(sc)

	var payload natsMessageWithContextOnInjection
	_ = json.Unmarshal(msg.Data, &payload)

	extractedSc, _ := payload.Sc.Extract()

	compareSpanContexts(t, extractedSc, &sc)
}

func createSpanAndRecorder() (zipkin.Span, *recorder.ReporterRecorder, *zipkin.Tracer) {
	rec := recorder.NewReporter()
	tr, _ := zipkin.NewTracer(rec)
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
