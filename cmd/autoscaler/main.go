package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// autoscaler é o análogo local do KEDA/HPA: monitora o lag de um consumer group e
// ajusta o nº de réplicas via docker-compose scale. Roda no HOST (precisa de acesso
// ao docker-compose). Em produção, esse papel é do KEDA ScaledObject no Kubernetes.
func main() {
	svc := env("AUTOSCALE_SERVICE", "orchestrator")
	group := env("AUTOSCALE_GROUP", "orchestrator")
	topic := env("AUTOSCALE_TOPIC", "orders.created")
	minR := intEnv("AUTOSCALE_MIN", 1)
	maxR := intEnv("AUTOSCALE_MAX", 3)
	highLag := intEnv("AUTOSCALE_HIGH_LAG", 500)
	idleDuration := time.Duration(intEnv("AUTOSCALE_IDLE_SECONDS", 30)) * time.Second
	checkInterval := time.Duration(intEnv("AUTOSCALE_CHECK_INTERVAL", 5)) * time.Second
	scaleStep := intEnv("AUTOSCALE_STEP", 1)

	ctx := context.Background()

	replicas := minR
	highCount := 0
	var idleStart time.Time

	log.Printf("autoscaler: iniciado svc=%s group=%s topic=%s min=%d max=%d high_lag=%d", svc, group, topic, minR, maxR, highLag)

	for {
		time.Sleep(checkInterval)

		lag, err := totalLag(ctx, group, topic)
		if err != nil {
			log.Printf("autoscaler: erro ao calcular lag: %v", err)
			continue
		}

		switch {
		case lag > highLag:
			highCount++
			idleStart = time.Time{}
			if highCount >= 2 && replicas < maxR {
				replicas += scaleStep
				if replicas > maxR {
					replicas = maxR
				}
				if err := scaleService(svc, replicas); err != nil {
					log.Printf("autoscaler: erro ao escalar: %v", err)
				}
				highCount = 0
			}
		case lag == 0:
			highCount = 0
			if idleStart.IsZero() {
				idleStart = time.Now()
			}
			if time.Since(idleStart) >= idleDuration && replicas > minR {
				replicas -= scaleStep
				if replicas < minR {
					replicas = minR
				}
				if err := scaleService(svc, replicas); err != nil {
					log.Printf("autoscaler: erro ao reduzir: %v", err)
				}
				idleStart = time.Time{}
			}
		default:
			highCount = 0
			idleStart = time.Time{}
		}

		log.Printf("autoscaler: svc=%s lag=%d replicas=%d high_count=%d", svc, lag, replicas, highCount)
	}
}

// totalLag soma o lag do grupo em um tópico usando a CLI do Kafka (ambiente docker-compose).
func totalLag(ctx context.Context, group string, topic string) (int, error) {
	cmd := exec.CommandContext(ctx, "docker-compose", "exec", "-T", "kafka", "/bin/sh", "-c",
		fmt.Sprintf("/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group %s", group))

	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}

	var total int
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		// Formato: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
		if len(fields) < 6 || fields[0] != group || fields[1] != topic {
			continue
		}
		if lag, err := strconv.Atoi(fields[5]); err == nil {
			total += lag
		}
	}
	return total, nil
}

// scaleService ajusta o nº de réplicas do serviço via docker-compose (roda no host).
func scaleService(svc string, replicas int) error {
	cmd := exec.Command("docker-compose", "up", "-d", "--no-deps", "--scale", svc+"="+strconv.Itoa(replicas), svc)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v: %s", err, string(out))
	}
	log.Printf("autoscaler: action=scale svc=%s replicas=%d", svc, replicas)
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
