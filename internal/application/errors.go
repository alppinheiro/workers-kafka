package application

import "errors"

// ErrNonRetryable marca erros definitivos: a mensagem é inválida ou não tem chance de
// sucesso em um novo processamento (ex.: evento fora de ordem genuíno, status inválido,
// poison pill). O consumer move tais mensagens para a DLQ e as commita.
var ErrNonRetryable = errors.New("erro definitivo: mensagem para DLQ")
