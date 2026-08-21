package orchestrator

import (
	"context"
	"errors"
	"sync"

	"workers-kafka/internal/application"
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

// inMemorySagaRepo é um fake de SagaRepository baseado em mapa, usado nos testes.
type inMemorySagaRepo struct {
	mu     sync.Mutex
	states map[string]domain.Saga
}

func newInMemorySagaRepo() *inMemorySagaRepo {
	return &inMemorySagaRepo{states: make(map[string]domain.Saga)}
}

func (r *inMemorySagaRepo) Save(_ context.Context, saga domain.Saga) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states[saga.OrderID] = saga
	return nil
}

func (r *inMemorySagaRepo) Load(_ context.Context, orderID string) (domain.Saga, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	saga, ok := r.states[orderID]
	if !ok {
		return domain.Saga{}, application.ErrSagaNotFound
	}
	return saga, nil
}

// mockFailingSagaRepo simula falhas no repositório de sagas para testar caminhos de erro.
type mockFailingSagaRepo struct{}

func (m *mockFailingSagaRepo) Save(context.Context, domain.Saga) error {
	return errors.New("falha simulada no save da saga")
}

func (m *mockFailingSagaRepo) Load(context.Context, string) (domain.Saga, error) {
	return domain.Saga{}, errors.New("falha simulada no load da saga")
}

// fakeEventLog captura as entradas do journal de eventos para asserções.
type fakeEventLog struct {
	entries []application.EventLogEntry
}

func (f *fakeEventLog) Append(_ context.Context, entry application.EventLogEntry) error {
	f.entries = append(f.entries, entry)
	return nil
}

// mockFailingEventLog simula falha na gravação do journal.
type mockFailingEventLog struct{}

func (m *mockFailingEventLog) Append(context.Context, application.EventLogEntry) error {
	return errors.New("falha simulada no journal")
}
