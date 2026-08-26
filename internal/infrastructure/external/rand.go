package external

import (
	"hash/fnv"
	"math/rand"
)

// randForOrder retorna um gerador pseudoaleatório DETERMINÍSTICO por orderID.
// Com o mesmo orderID, o resultado (aprovado/recusado) é sempre o mesmo — consistente
// entre instâncias do worker (review 2.3: o rng global por simulador tornava o fluxo
// imprevisível em scale horizontal). O cenário retry-once (contagem de tentativas em
// memória) permanece documentado como limitação de estudo.
func randForOrder(orderID string) *rand.Rand {
	h := fnv.New32a()
	_, _ = h.Write([]byte(orderID))
	return rand.New(rand.NewSource(int64(h.Sum32())))
}
