package pkg

import (
	"github.com/go-kit/kit/log"
	"net/http"
)

type TracerOption func(o *tracerOptions)

type tracerOptions struct {
	tags           map[string]string
	name           string
	logger         log.Logger
	propagate      bool
	requestSampler func(r *http.Request) bool
}

func SetName(name string) TracerOption {
	return func(o *tracerOptions) {
		o.name = name
	}
}
