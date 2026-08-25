package postgres

import (
	"testing"
	"time"
)

func TestPoolEnvDefaults(t *testing.T) {
	t.Setenv("DATABASE_POOL_MAX_CONNS", "")
	t.Setenv("DATABASE_POOL_MIN_CONNS", "")
	t.Setenv("DATABASE_POOL_MAX_LIFETIME", "")
	t.Setenv("DATABASE_POOL_IDLE_TIMEOUT", "")

	if got := PoolMaxConnsFromEnv(); got != 10 {
		t.Fatalf("MaxConns default deveria ser 10, got %d", got)
	}
	if got := PoolMinConnsFromEnv(); got != 2 {
		t.Fatalf("MinConns default deveria ser 2, got %d", got)
	}
	if got := PoolMaxLifetimeFromEnv(); got != time.Hour {
		t.Fatalf("MaxLifetime default deveria ser 1h, got %s", got)
	}
	if got := PoolIdleTimeoutFromEnv(); got != 30*time.Second {
		t.Fatalf("IdleTimeout default deveria ser 30s, got %s", got)
	}
}

func TestPoolEnvCustom(t *testing.T) {
	t.Setenv("DATABASE_POOL_MAX_CONNS", "25")
	t.Setenv("DATABASE_POOL_MIN_CONNS", "4")
	t.Setenv("DATABASE_POOL_MAX_LIFETIME", "90s")
	t.Setenv("DATABASE_POOL_IDLE_TIMEOUT", "10s")

	if got := PoolMaxConnsFromEnv(); got != 25 {
		t.Fatalf("MaxConns custom deveria ser 25, got %d", got)
	}
	if got := PoolMinConnsFromEnv(); got != 4 {
		t.Fatalf("MinConns custom deveria ser 4, got %d", got)
	}
	if got := PoolMaxLifetimeFromEnv(); got != 90*time.Second {
		t.Fatalf("MaxLifetime custom deveria ser 90s, got %s", got)
	}
	if got := PoolIdleTimeoutFromEnv(); got != 10*time.Second {
		t.Fatalf("IdleTimeout custom deveria ser 10s, got %s", got)
	}
}

func TestPoolEnvInvalidFallsBack(t *testing.T) {
	t.Setenv("DATABASE_POOL_MAX_CONNS", "abc")
	t.Setenv("DATABASE_POOL_MAX_LIFETIME", "xpto")

	if got := PoolMaxConnsFromEnv(); got != 10 {
		t.Fatalf("MaxConns inválido deveria cair para 10, got %d", got)
	}
	if got := PoolMaxLifetimeFromEnv(); got != time.Hour {
		t.Fatalf("MaxLifetime inválido deveria cair para 1h, got %s", got)
	}
}
