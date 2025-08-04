package internal

import (
	"context"
	"net/http"
	"net/url"

	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/support/render/problem"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("friendbot_tracer")

// FriendbotHandler causes an account at `Address` to be created.
type FriendbotHandler struct {
	Friendbot *Bot
}

// Handle is a method that implements http.HandlerFunc
func (handler *FriendbotHandler) Handle(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "friendbot.handle_request")
	defer span.End()
	// Add request attributes to span
	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.url", r.URL.String()),
		attribute.String("http.user_agent", r.UserAgent()),
	)
	result, err := handler.doHandle(ctx, r)
	if err != nil {
		problem.Render(r.Context(), w, err)
		return
	}

	hal.Render(w, *result)
}

// doHandle is just a convenience method that returns the object to be rendered
func (handler *FriendbotHandler) doHandle(ctx context.Context, r *http.Request) (*horizon.Transaction, error) {
	ctx, span := tracer.Start(ctx, "friendbot.do_handle_request")
	defer span.End()
	err := r.ParseForm()
	if err != nil {
		p := problem.BadRequest
		p.Detail = "Request parameters are not escaped or incorrectly formatted."
		return nil, &p
	}

	address, err := handler.loadAddress(ctx, r)
	if err != nil {
		return nil, problem.MakeInvalidFieldProblem("addr", err)
	}
	return handler.Friendbot.Pay(ctx, address)
}

func (handler *FriendbotHandler) loadAddress(ctx context.Context, r *http.Request) (string, error) {
	_, span := tracer.Start(ctx, "friendbot.load_address")
	defer span.End()

	address := r.Form.Get("addr")
	if address == "" {
		span.SetStatus(codes.Error, "missing destination account address")
		span.SetAttributes(attribute.String("error.type", "missing_parameter"))
	}

	unescaped, err := url.QueryUnescape(address)
	if err != nil {
		return unescaped, err
	}

	_, err = strkey.Decode(strkey.VersionByteAccountID, unescaped)
	span.SetAttributes(attribute.String("destination.account", address))
	return unescaped, err
}
