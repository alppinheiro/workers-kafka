package postgres_read

import "os"

// DatabaseURLFromEnv lê a URL de conexão do banco de leitura da variável DATABASE_URL,
// usando um default conveniente para desenvolvimento local.
func DatabaseURLFromEnv() string {
	raw := os.Getenv("DATABASE_URL")
	if raw == "" {
		return "postgres://saga:saga@localhost:5432/saga_read?sslmode=disable"
	}
	return raw
}
