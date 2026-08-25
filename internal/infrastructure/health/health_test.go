package health

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakePinger struct {
	err error
}

func (f *fakePinger) Ping(context.Context) error { return f.err }

func TestPostgres_OkAndFail(t *testing.T) {
	ok := Postgres(&fakePinger{})
	if err := ok(context.Background()); err != nil {
		t.Fatalf("ping ok deveria passar, got %v", err)
	}

	bad := Postgres(&fakePinger{err: errors.New("connection down")})
	if err := bad(context.Background()); err == nil {
		t.Fatal("ping com erro deveria falhar a check")
	}
}

func TestLastActivity(t *testing.T) {
	var last atomic.Int64
	check := LastActivity(&last, 30*time.Second)

	// Nenhum ciclo concluído ainda.
	if err := check(context.Background()); err == nil {
		t.Fatal("sem atividade deveria falhar a check")
	}

	// Atividade mais antiga que o limite.
	last.Store(time.Now().Add(-time.Minute).UnixNano())
	if err := check(context.Background()); err == nil {
		t.Fatal("atividade stale deveria falhar a check")
	}

	// Atividade recente.
	last.Store(time.Now().UnixNano())
	if err := check(context.Background()); err != nil {
		t.Fatalf("atividade recente deveria passar, got %v", err)
	}
}
