package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Init configura o OpenTelemetry com exporter OTLP/HTTP (Jaeger) e propagação W3C
// Trace Context, retornando a função de shutdown a ser chamada no encerramento do serviço.
// O endpoint é lido de OTEL_EXPORTER_OTLP_ENDPOINT (default localhost:4318) e o
// sampler de OTEL_TRACES_SAMPLER (+ OTEL_TRACES_SAMPLER_ARG), conforme a spec.
func Init(serviceName string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4318"
	}

	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(samplerFromEnv()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	slog.Info("telemetria iniciada", "component", "telemetry", "phase", "started", "service", serviceName, "endpoint", endpoint)
	return tp.Shutdown, nil
}

// samplerFromEnv resolve o sampler a partir das variáveis padrão da spec OTel
// (OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG). Default: parentbased_always_on
// (raiz sempre amostrada; filhos seguem a decisão do pai) — preserva o
// comportamento de estudo. Em produção use parentbased_traceidratio + ARG (ex.: 0.1).
func samplerFromEnv() sdktrace.Sampler {
	sampler := os.Getenv("OTEL_TRACES_SAMPLER")
	arg := os.Getenv("OTEL_TRACES_SAMPLER_ARG")

	ratio := 1.0
	if f, err := strconv.ParseFloat(arg, 64); err == nil && f >= 0 && f <= 1 {
		ratio = f
	}

	switch sampler {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio)
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	case "parentbased_always_on", "":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	default:
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
}
