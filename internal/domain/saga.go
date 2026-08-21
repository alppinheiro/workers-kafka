package domain

// Saga representa o estado lógico de uma saga de pedido em andamento, persistido
// entre reinícios do orquestrador para permitir recuperação do ponto onde parou.
type Saga struct {
	OrderID       string
	Previous      OrderStatus
	Current       OrderStatus
	RetryCount    int
	TransactionID string
}
