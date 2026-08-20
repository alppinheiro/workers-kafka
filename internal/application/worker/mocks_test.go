package worker

import (
	"context"
	"errors"

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

var errSimulado = errors.New("erro simulado do gateway")
