package projector

import (
	"context"

	"workers-kafka/internal/application"
	"workers-kafka/internal/domain"
)

// Projector aplica eventos do barramento no read model do banco de leitura, com dedup
// por event_id (adequado ao modelo at-least-once do Kafka).
type Projector struct {
	views application.OrderViewRepository
}

// New cria um projector sobre o repositório do read model.
func New(views application.OrderViewRepository) *Projector {
	return &Projector{views: views}
}

// HandleEvent é o handler do consumer de projeção: ignora eventos já processados
// (redelivery) e aplica os demais no read model.
func (p *Projector) HandleEvent(ctx context.Context, event domain.Event) error {
	first, err := p.views.MarkProcessed(ctx, event.EventID)
	if err != nil {
		return err
	}
	if !first {
		return nil // evento já processado (redelivery)
	}
	return p.views.ApplyEvent(ctx, event)
}
