package kafka

import (
	"testing"

	"workers-kafka/internal/domain"
)

func TestValidateSchemaVersion(t *testing.T) {
	ok := domain.Event{EventID: "e1", OrderID: "o1", SchemaVersion: domain.CurrentSchemaVersion}
	if err := validateSchemaVersion(ok); err != nil {
		t.Fatalf("schema atual deveria passar, got %v", err)
	}

	// Schema desconhecido (contrato futuro/incompatível) deve ser rejeitado.
	unknown := domain.Event{EventID: "e2", OrderID: "o2", SchemaVersion: domain.CurrentSchemaVersion + 1}
	err := validateSchemaVersion(unknown)
	if err == nil {
		t.Fatal("schema desconhecido deveria falhar a validação")
	}

	// Schema 0 (evento malformado) também é rejeitado.
	zero := domain.Event{EventID: "e3", OrderID: "o3", SchemaVersion: 0}
	if err := validateSchemaVersion(zero); err == nil {
		t.Fatal("schema 0 deveria falhar a validação")
	}
}
