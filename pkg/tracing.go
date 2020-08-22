package pkg

import (
	"context"
	"github.com/go-kit/kit/log"
	kitnats "github.com/go-kit/kit/transport/nats"
	"github.com/nats-io/nats.go"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
)

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
			zipkin.Kind(model.Server),
			zipkin.Tags(config.tags),
			zipkin.Parent(spanContext),
			zipkin.FlushOnFinish(false),
		)

		return zipkin.NewContext(ctx, span)
	})

	subscriberAfter := kitnats.SubscriberAfter(
		func(ctx context.Context, conn *nats.Conn) context.Context {
			// TODO trace errors somehow
			if span := zipkin.SpanFromContext(ctx); span != nil {
				span.Finish()
			}

			return ctx
		})

	finalizer := kitnats.SubscriberFinalizer(func(ctx context.Context, msg *nats.Msg) {
		if span := zipkin.SpanFromContext(ctx); span != nil {
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
			zipkin.Kind(model.Client),
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
