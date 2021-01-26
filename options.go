package dbwrap

import (
	"context"
	"database/sql/driver"
)

// Middleware returns instrumented context and finalizer callback.
//
// Middleware is invoked before operation.
// Returned onFinish function is invoked after the operation.
type Middleware func(
	ctx context.Context,
	operation Operation,
	statement string,
	args []driver.NamedValue,
) (nCtx context.Context, onFinish func(error))

// Option allows for managing wrapper configuration using functional options.
type Option func(o *Options)

// Options holds configuration of our wrapper.
// By default all options are set to false intentionally when creating a wrapped
// driver and provide the most sensible default with both performance and
// security in mind.
type Options struct {
	// Middlewares wrap operations.
	Middlewares []Middleware

	// Intercept mutates statement and/or parameters.
	Intercept func(
		ctx context.Context,
		operation Operation,
		statement string,
		args []driver.NamedValue,
	) (context.Context, string, []driver.NamedValue)

	// Operations lists which operations should be wrapped.
	Operations []Operation

	operations map[Operation]bool
}

// WithOptions sets our wrapper options through a single
// Options object.
func WithOptions(options Options) Option {
	return func(o *Options) {
		*o = options
	}
}

// WithMiddleware adds one or multiple middlewares to a db wrapper.
func WithMiddleware(mw ...Middleware) Option {
	return func(o *Options) {
		o.Middlewares = append(o.Middlewares, mw...)
	}
}

// WithInterceptor sets statement interceptor to a db wrapper.
// Interceptor receives every statement that is to be requested
// and can change it.
func WithInterceptor(i func(
	ctx context.Context,
	operation Operation,
	statement string,
	args []driver.NamedValue,
) (context.Context, string, []driver.NamedValue)) Option {
	return func(o *Options) {
		o.Intercept = i
	}
}

// WithOperations controls which operations should be wrapped with middlewares.
// It does not affect statement interceptor.
func WithOperations(op ...Operation) Option {
	return func(o *Options) {
		o.Operations = append(o.Operations, op...)
	}
}

// prepareOptions returns prepared Options and flag if they are operational.
func prepareOptions(options []Option) (Options, bool) {
	o := Options{}

	for _, option := range options {
		option(&o)
	}

	if len(o.Middlewares) == 0 && o.Intercept == nil {
		return o, false
	}

	o.operations = defaultOperations

	if len(o.Operations) > 0 {
		o.operations = make(map[Operation]bool, len(o.Operations))

		for _, op := range o.Operations {
			o.operations[op] = true
		}
	}

	return o, true
}
