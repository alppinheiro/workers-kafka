package orchestrator

import (
	"context"
	"errors"

	"workers-kafka/internal/domain"
)

// mockPublisher é um fake que captura todos os eventos publicados para asserções nos testes.
type mockPublisher struct {
	events []domain.Event
}

func (m *mockPublisher) Publish(_ context.Context, event domain.Event) error {
	m.events = append(m.events, event)
	return nil
}

// mockFailingPublisher simula uma falha no publish para testar os caminhos de erro.
type mockFailingPublisher struct{}

func (m *mockFailingPublisher) Publish(_ context.Context, _ domain.Event) error {
	return errors.New("falha simulada no publish")
}
