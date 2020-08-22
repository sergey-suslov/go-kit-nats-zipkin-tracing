package pkg

import (
	"context"
	"github.com/go-kit/kit/log"
	kitnats "github.com/go-kit/kit/transport/nats"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
)

// NATSSubscriberTrace enables native Zipkin tracing of a Go kit NATS transport
// Subscriber.
//
// Go kit creates NATS transport subscriber per remote endpoint. This option
// will create a span on every request. This option will also transform every
// nats.Msg, parsing it as natsMessageWithContext and passing Data value as
// nats.Msg Data further or will ignore it if nats.Msg Data is not the shape
// of natsMessageWithContext. The option will parse span context from the
// nats.Msg Data and use it as the parent context. It is safe using this option
// with the endpoints that may send not natsMessageWithContext typed messages.
func NATSSubscriberTrace(tracer *zipkin.Tracer, options ...TracerOption) kitnats.SubscriberOption {
	config := tracerOptions{
		tags:      make(map[string]string),
		name:      "",
		logger:    log.NewNopLogger(),
		propagate: true,
	}

	for _, option := range options {
		option(&config)
	}

	subscriberBefore := kitnats.SubscriberBefore(func(ctx context.Context, msg *nats.Msg) context.Context {
		var (
			spanContext model.SpanContext
			name        string
		)

		if config.name != "" {
			name = config.name
		} else {
			name = msg.Subject
		}

		if config.propagate {
			spanContext = tracer.Extract(ExtractNATS(msg))
			if spanContext.Err != nil {
				_ = config.logger.Log("err", spanContext.Err)
			}
		}

		span := tracer.StartSpan(
			name,
			zipkin.Kind(model.Consumer),
			zipkin.Tags(config.tags),
			zipkin.Parent(spanContext),
			zipkin.FlushOnFinish(false),
		)

		return zipkin.NewContext(ctx, span)
	})

	subscriberAfter := kitnats.SubscriberAfter(
		func(ctx context.Context, conn *nats.Conn) context.Context {
			if span := zipkin.SpanFromContext(ctx); span != nil {
				span.Finish()
			}

			return ctx
		})

	finalizer := kitnats.SubscriberFinalizer(func(ctx context.Context, msg *nats.Msg) {
		if span := zipkin.SpanFromContext(ctx); span != nil {
			if config.errChecker != nil {
				checkForErrorAndWriteSpanTag(span, msg, config.errChecker)
			}
			span.Finish()
			span.Flush()
		}
	})

	return func(subscriber *kitnats.Subscriber) {
		subscriberBefore(subscriber)
		subscriberAfter(subscriber)
		finalizer(subscriber)
	}
}

// NATSPublisherTrace enables native Zipkin tracing of a Go kit NATS transport
// Publisher.
//
// Go kit creates NATS transport publisher per remote endpoint. This option
// will create a span on every request. This option will also transform every
// sending nats.Msg, moving nats.Msg.Data into a separate field (Data) in the
// natsMessageWithContext struct. This middleware will also create a span on
// every request and add its context (using Inject method of b3.Map) to the
// nats.Msg as Sc field. In case of sending a request to a service that is not
// using NATSSubscriberTrace option, use AllowPropagation TracerOption to
// disallow propagation.
func NATSPublisherTrace(tracer *zipkin.Tracer, options ...TracerOption) kitnats.PublisherOption {
	config := tracerOptions{
		tags:      make(map[string]string),
		name:      "",
		logger:    log.NewNopLogger(),
		propagate: true,
	}

	for _, option := range options {
		option(&config)
	}

	publisherBefore := kitnats.PublisherBefore(func(ctx context.Context, msg *nats.Msg) context.Context {
		var (
			spanContext model.SpanContext
			name        string
		)

		if config.name != "" {
			name = config.name
		} else {
			name = msg.Subject
		}

		if parent := zipkin.SpanFromContext(ctx); parent != nil {
			spanContext = parent.Context()
		}

		span := tracer.StartSpan(
			name,
			zipkin.Kind(model.Producer),
			zipkin.Tags(config.tags),
			zipkin.Parent(spanContext),
			zipkin.FlushOnFinish(false),
		)

		if config.propagate {
			if err := InjectNATS(msg)(span.Context()); err != nil {
				_ = config.logger.Log("err", err)
			}
		}

		return zipkin.NewContext(ctx, span)
	})

	publisherAfter := kitnats.PublisherAfter(func(ctx context.Context, msg *nats.Msg) context.Context {
		if span := zipkin.SpanFromContext(ctx); span != nil {
			if config.errChecker != nil {
				checkForErrorAndWriteSpanTag(span, msg, config.errChecker)
			}
			span.Finish()
			span.Flush()
		}

		return ctx
	})

	return func(publisher *kitnats.Publisher) {
		publisherBefore(publisher)
		publisherAfter(publisher)
	}
}

func checkForErrorAndWriteSpanTag(span zipkin.Span, msg *nats.Msg, checker ErrorChecker) {
	if err := checker(msg); err != nil {
		zipkin.TagError.Set(span, err.Error())
	}
}
