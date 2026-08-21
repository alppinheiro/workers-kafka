package worker

import (
	"context"
	"errors"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// resultEvent cria um evento genérico com valores mínimos para os testes.
func resultEvent(orderID string, eventType domain.EventType, status domain.OrderStatus) domain.Event {
	return domain.Event{
		EventID:       "evt-gen",
		OrderID:       orderID,
		SagaID:        orderID,
		StatusAtual:   status,
		EventType:     eventType,
		SchemaVersion: domain.CurrentSchemaVersion,
	}
}

// mockPublisher captura os eventos publicados para asserções nos testes.
type mockPublisher struct {
	events []domain.Event
}

func (m *mockPublisher) Publish(_ context.Context, event domain.Event) error {
	m.events = append(m.events, event)
	return nil
}

// mockPaymentGateway simula a API externa de pagamento com comportamento configurável.
type mockPaymentGateway struct {
	approved      bool
	transactionID string
	err           error
	refunded      bool
	refundErr     error
}

func (m *mockPaymentGateway) Process(_ context.Context, _ string) (bool, string, error) {
	return m.approved, m.transactionID, m.err
}

func (m *mockPaymentGateway) Refund(_ context.Context, _ string, _ string) (bool, error) {
	return m.refunded, m.refundErr
}

// mockInventoryGateway simula a API externa de estoque.
type mockInventoryGateway struct {
	reserved bool
	err      error
}

func (m *mockInventoryGateway) Reserve(_ context.Context, _ string) (bool, error) {
	return m.reserved, m.err
}

// mockNotificationGateway simula a API externa de notificação.
type mockNotificationGateway struct {
	sent bool
	err  error
}

func (m *mockNotificationGateway) Notify(_ context.Context, _ string) (bool, error) {
	return m.sent, m.err
}

// fakeEventLog captura as entradas do journal de eventos para asserções.
type fakeEventLog struct {
	entries []application.EventLogEntry
}

func (f *fakeEventLog) Append(_ context.Context, entry application.EventLogEntry) error {
	f.entries = append(f.entries, entry)
	return nil
}

func (f *fakeEventLog) Has(_ context.Context, eventID string, component string) (bool, error) {
	for _, e := range f.entries {
		if e.EventID == eventID && e.Component == component {
			return true, nil
		}
	}
	return false, nil
}

var errSimulado = errors.New("erro simulado do gateway")

// fakeUoW executa o bloco de trabalho imediatamente com os fakes, simulando a transação
// sem banco real (o rollback é no-op; os testes de erro verificam o comportamento).
type fakeUoW struct {
	eventLog application.EventLogRepository
	pub      application.EventPublisher
}

// uowWith monta um fakeUoW ligado aos fakes informados, permitindo asserções sobre o
// journal (fakeEventLog) e os eventos publicados (mockPublisher).
func uowWith(pub application.EventPublisher, eventLog application.EventLogRepository) application.SagaUnitOfWork {
	return &fakeUoW{eventLog: eventLog, pub: pub}
}

func (u *fakeUoW) WithTx(_ context.Context, fn func(application.SagaTx) error) error {
	return fn(application.SagaTx{EventLog: u.eventLog, Publisher: u.pub})
}
