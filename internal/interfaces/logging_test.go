package interfaces

import (
	"context"
	"errors"
	"testing"

	"workers-kafka/internal/domain"
)

func sampleEvent() domain.Event {
	return domain.Event{
		EventID:        "evt-1",
		OrderID:        "order-001",
		SagaID:         "order-001",
		TransactionID:  "tx-1",
		StatusAnterior: domain.StatusPending,
		StatusAtual:    domain.StatusPaymentPending,
		EventType:      domain.EventPaymentCommand,
		SchemaVersion:  1,
		Metadata:       map[string]string{"k1": "v1", "k2": "v2"},
	}
}

func TestFormatMetadata_Empty(t *testing.T) {
	if got := formatMetadata(nil); got != "-" {
		t.Errorf("esperado '-', obtido %q", got)
	}
	if got := formatMetadata(map[string]string{}); got != "-" {
		t.Errorf("esperado '-' para mapa vazio, obtido %q", got)
	}
}

func TestFormatMetadata_Sorted(t *testing.T) {
	meta := map[string]string{"b": "2", "a": "1", "c": "3"}
	got := formatMetadata(meta)
	expected := "a=1,b=2,c=3"
	if got != expected {
		t.Errorf("esperado %q, obtido %q", expected, got)
	}
}

func TestWithLogging_Success(t *testing.T) {
	called := false
	next := func(_ context.Context, _ domain.Event) error {
		called = true
		return nil
	}

	handler := WithLogging("test-worker", next)
	err := handler(context.Background(), sampleEvent())
	if err != nil {
		t.Fatalf("handler retornou erro inesperado: %v", err)
	}
	if !called {
		t.Error("handler interno não foi chamado")
	}
}

func TestWithLogging_ErrorPropagates(t *testing.T) {
	sentinel := errors.New("falha no worker")
	next := func(_ context.Context, _ domain.Event) error {
		return sentinel
	}

	handler := WithLogging("test-worker", next)
	err := handler(context.Background(), sampleEvent())
	if !errors.Is(err, sentinel) {
		t.Errorf("esperado erro do handler interno, obtido %v", err)
	}
}
