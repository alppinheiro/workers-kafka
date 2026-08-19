package domain

// OrderStatus representa a etapa atual do ciclo de vida do pedido na saga.
type OrderStatus string

const (
	StatusPending           OrderStatus = "PENDING"
	StatusPaymentPending    OrderStatus = "PAYMENT_PENDING"
	StatusPaymentApproved   OrderStatus = "PAYMENT_APPROVED"
	StatusInventoryReserved OrderStatus = "INVENTORY_RESERVED"
	StatusNotified          OrderStatus = "NOTIFIED"
	StatusCompleted         OrderStatus = "COMPLETED"
	StatusRetrying          OrderStatus = "RETRYING"
	StatusFailed            OrderStatus = "FAILED"
)
