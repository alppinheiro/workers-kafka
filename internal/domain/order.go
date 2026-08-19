package domain

// Order representa o pedido cujo ciclo de vida é coordenado pela saga.
type Order struct {
	ID     string
	Status OrderStatus
}
