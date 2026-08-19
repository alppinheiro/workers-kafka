package domain

import (
	"crypto/rand"
	"fmt"
)

// NewEventID gera um identificador aleatório para correlacionar eventos, sem depender de bibliotecas externas.
func NewEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
