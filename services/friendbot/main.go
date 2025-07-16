package main

import (
	"context"
	"database/sql"
	"fmt"
	stdhttp "net/http"
	"os"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"github.com/stellar/go/services/friendbot/internal"
	"github.com/stellar/go/support/app"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/http"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/problem"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
)

const (
	serviceName    = "stellar-friendbot"
	serviceVersion = "1.0.0" //TODO: Change version
)

// Config represents the configuration of a friendbot server
type Config struct {
	Port                   int         `toml:"port" valid:"required"`
	FriendbotSecret        string      `toml:"friendbot_secret" valid:"required"`
	NetworkPassphrase      string      `toml:"network_passphrase" valid:"required"`
	HorizonURL             string      `toml:"horizon_url" valid:"required"`
	StartingBalance        string      `toml:"starting_balance" valid:"required"`
	TLS                    *config.TLS `valid:"optional"`
	NumMinions             int         `toml:"num_minions" valid:"optional"`
	BaseFee                int64       `toml:"base_fee" valid:"optional"`
	MinionBatchSize        int         `toml:"minion_batch_size" valid:"optional"`
	SubmitTxRetriesAllowed int         `toml:"submit_tx_retries_allowed" valid:"optional"`
	UseCloudflareIP        bool        `toml:"use_cloudflare_ip" valid:"optional"`
	OtelEnabled            bool        `toml: "otel_enabled" valid:"optional"`
	OtelEndpoint           string      `toml: "otel_endpoint" valid:"optional"`
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "friendbot",
		Short: "friendbot for the Stellar Test Network",
		Long:  "Client-facing API server for the friendbot service on the Stellar Test Network",
		Run:   run,
	}

	rootCmd.PersistentFlags().String("conf", "./friendbot.cfg", "config file path")
	rootCmd.Execute()
}

func run(cmd *cobra.Command, args []string) {
	var (
		cfg     Config
		cfgPath = cmd.PersistentFlags().Lookup("conf").Value.String()
	)
	log.SetLevel(log.InfoLevel)

	err := config.Read(cfgPath, &cfg)
	if err != nil {
		switch cause := errors.Cause(err).(type) {
		case *config.InvalidConfigError:
			log.Error("config file: ", cause)
		default:
			log.Error(err)
		}
		os.Exit(1)
	}

	//Initialize open telemetry
	tracer, err := initTracer(&cfg)
	if err != nil {
		log.Fatal("Failed to initialize tracer:", err)
	}
	defer tracer()

	fb, err := initFriendbot(cfg.FriendbotSecret, cfg.NetworkPassphrase, cfg.HorizonURL, cfg.StartingBalance,
		cfg.NumMinions, cfg.BaseFee, cfg.MinionBatchSize, cfg.SubmitTxRetriesAllowed)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	router := initRouter(cfg, fb)
	registerProblems()

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Port)

	http.Run(http.Config{
		ListenAddr: addr,
		Handler:    router,
		TLS:        cfg.TLS,
		OnStarting: func() {
			log.Infof("starting friendbot server - %s", app.Version())
			log.Infof("listening on %s", addr)
		},
	})
}

func initRouter(cfg Config, fb *internal.Bot) *chi.Mux {
	mux := newMux(cfg)

	handler := internal.NewFriendbotHandler(fb, cfg.OtelEnabled)

	mux.Get("/", handler.Handle)
	mux.Post("/", handler.Handle)
	mux.NotFound(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		problem.Render(r.Context(), w, problem.NotFound)
	}))

	return mux
}

func newMux(cfg Config) *chi.Mux {
	mux := chi.NewRouter()
	// first apply XFFMiddleware so we can have the real ip in the subsequent
	// middlewares
	mux.Use(http.XFFMiddleware(http.XFFMiddlewareConfig{BehindCloudflare: cfg.UseCloudflareIP}))
	mux.Use(http.NewAPIMux(log.DefaultLogger).Middlewares()...)

	// Extract information using middleware
	mux.Use(middleware.RequestID)
	mux.Use(middleware.RealIP)
	mux.Use(middleware.Logger)
	mux.Use(middleware.Recoverer)

	// Add OpenTelemetry middleware if enabled
	if cfg.OtelEnabled {
		mux.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(mux)))
	}

	return mux
}

func registerProblems() {
	problem.RegisterError(sql.ErrNoRows, problem.NotFound)

	accountExistsProblem := problem.BadRequest
	accountExistsProblem.Detail = internal.ErrAccountExists.Error()
	problem.RegisterError(internal.ErrAccountExists, accountExistsProblem)

	accountFundedProblem := problem.BadRequest
	accountFundedProblem.Detail = internal.ErrAccountFunded.Error()
	problem.RegisterError(internal.ErrAccountFunded, accountFundedProblem)
}

func initTracer(cfg *Config) (func(), error) {
	ctx := context.Background()

	if !cfg.OtelEnabled {
		log.Info("OpenTelemetry tracing is disabled")
		return func() {}, nil
	}

	// Create resource
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.OtelEndpoint),
		otlptracehttp.WithInsecure(),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	//Create a new traceprovider
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(traceProvider)

	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := traceProvider.Shutdown(ctx); err != nil {
			log.Error("Error shutting down tracer provider", err)
		}
	}, nil
}
