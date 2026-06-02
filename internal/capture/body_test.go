package capture

import (
	"testing"

	"github.com/jmanzano/mq-lens/internal/domain"
)

func TestDetectBody(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want domain.BodyFormat
	}{
		{name: "empty", body: nil, want: domain.BodyEmpty},
		{name: "json object", body: []byte(`{"id":1}`), want: domain.BodyJSON},
		{name: "json array", body: []byte(`[{"id":1}]`), want: domain.BodyJSON},
		{name: "xml", body: []byte(`<order><id>1</id></order>`), want: domain.BodyXML},
		{name: "text", body: []byte(`plain text`), want: domain.BodyText},
		{name: "bytes", body: []byte{0xff, 0x00, 0x01}, want: domain.BodyBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DetectBody(tt.body); got != tt.want {
				t.Fatalf("DetectBody()=%s want %s", got, tt.want)
			}
		})
	}
}

func TestProcessBodyRedactsAndTruncates(t *testing.T) {
	got := ProcessBody([]byte(`{"password":"open","name":"order"}`), 12, true)
	if !got.Truncated {
		t.Fatalf("expected truncation")
	}
	if got.Size != 34 {
		t.Fatalf("size=%d", got.Size)
	}
}

func TestRedactMap(t *testing.T) {
	got, changed := RedactMap(map[string]string{"authorization": "secret", "x": "y"})
	if !changed {
		t.Fatalf("expected changed")
	}
	if got["authorization"] != redactedValue {
		t.Fatalf("authorization was not redacted")
	}
	if got["x"] != "y" {
		t.Fatalf("unexpected non-sensitive redaction")
	}
}
