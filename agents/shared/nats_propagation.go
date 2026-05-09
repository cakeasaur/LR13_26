package shared

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/nats-io/nats.go"
)

// InjectContext записывает W3C TraceContext из ctx в заголовки NATS-сообщения.
// msg.Header инициализируется если nil.
func InjectContext(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))
}

// ExtractContext извлекает W3C TraceContext из заголовков NATS-сообщения и
// возвращает новый контекст с parent span. Если заголовков нет — возвращает ctx без изменений.
func ExtractContext(ctx context.Context, msg *nats.Msg) context.Context {
	if len(msg.Header) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))
}

// PublishWithContext публикует data в subject, вложив trace context из ctx в заголовки.
func PublishWithContext(ctx context.Context, nc *nats.Conn, subject string, data []byte) error {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	InjectContext(ctx, msg)
	return nc.PublishMsg(msg)
}
