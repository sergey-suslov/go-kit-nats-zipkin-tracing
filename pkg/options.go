package pkg

import (
	"github.com/go-kit/kit/log"
	"github.com/nats-io/nats.go"
)

type TracerOption func(o *tracerOptions)
type ErrorChecker func(msg *nats.Msg) error

type tracerOptions struct {
	tags       map[string]string
	name       string
	logger     log.Logger
	propagate  bool
	errChecker ErrorChecker
}

// All the TracerOption functions below were taken from the tracing/zipkin/options.go file of the go-kit repo
func Name(name string) TracerOption {
	return func(o *tracerOptions) {
		o.name = name
	}
}

func Tags(tags map[string]string) TracerOption {
	return func(o *tracerOptions) {
		for k, v := range tags {
			o.tags[k] = v
		}
	}
}

func Logger(logger log.Logger) TracerOption {
	return func(o *tracerOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

func AllowPropagation(propagate bool) TracerOption {
	return func(o *tracerOptions) {
		o.propagate = propagate
	}
}

// ErrChecker allows setting of errChecker function. This function, if present,
// will be used to check response message on errors. If if returns an error, zipkin.TagError
// tag will be added to a span.
func ErrChecker(checker ErrorChecker) TracerOption {
	return func(o *tracerOptions) {
		o.errChecker = checker
	}
}
