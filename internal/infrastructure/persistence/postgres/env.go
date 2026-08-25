package postgres

import (
	"os"
	"strconv"
	"time"
)

// DatabaseURLFromEnv lê a URL de conexão do banco de escrita da variável DATABASE_URL,
// usando um default conveniente para desenvolvimento local.
func DatabaseURLFromEnv() string {
	raw := os.Getenv("DATABASE_URL")
	if raw == "" {
		return "postgres://saga:saga@localhost:5432/saga?sslmode=disable"
	}
	return raw
}

// PoolMaxConnsFromEnv lê o teto de conexões do pool (DATABASE_POOL_MAX_CONNS).
// Default 10: no stress de 120k, o default do pgx (max(4, NumCPU)) subdimensiona o
// pipeline inteiro (orquestrador + workers + relay + exporter compartilham o Postgres).
func PoolMaxConnsFromEnv() int32 {
	raw := os.Getenv("DATABASE_POOL_MAX_CONNS")
	if raw == "" {
		return 10
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 10
	}
	return int32(n)
}

// PoolMinConnsFromEnv lê o mínimo de conexões mantidas abertas (DATABASE_POOL_MIN_CONNS,
// default 2) — evita o custo de abrir conexões a cada pico.
func PoolMinConnsFromEnv() int32 {
	raw := os.Getenv("DATABASE_POOL_MIN_CONNS")
	if raw == "" {
		return 2
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 2
	}
	return int32(n)
}

// PoolMaxLifetimeFromEnv lê o tempo de vida máximo de uma conexão
// (DATABASE_POOL_MAX_LIFETIME, default 1h) — evita conexões envelhecidas atrás de
// proxies/firewalls.
func PoolMaxLifetimeFromEnv() time.Duration {
	raw := os.Getenv("DATABASE_POOL_MAX_LIFETIME")
	if raw == "" {
		return time.Hour
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < time.Minute {
		return time.Hour
	}
	return d
}

// PoolIdleTimeoutFromEnv lê o tempo de ociosidade antes de fechar uma conexão
// (DATABASE_POOL_IDLE_TIMEOUT, default 30s).
func PoolIdleTimeoutFromEnv() time.Duration {
	raw := os.Getenv("DATABASE_POOL_IDLE_TIMEOUT")
	if raw == "" {
		return 30 * time.Second
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < time.Second {
		return 30 * time.Second
	}
	return d
}
