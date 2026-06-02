package capture

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/jmanzano/mq-lens/internal/domain"
)

const redactedValue = "***REDACTED***"

var sensitiveKey = regexp.MustCompile(`(?i)(password|passwd|secret|token|access_token|refresh_token|authorization|api_key|apikey|credential|private_key|client_secret)`)

type ProcessedBody struct {
	Format    domain.BodyFormat
	Text      string
	Bytes     []byte
	Size      int64
	SHA256    string
	Truncated bool
	Redacted  bool
}

func ProcessBody(body []byte, maxBytes int64, redact bool) ProcessedBody {
	sum := sha256.Sum256(body)
	size := int64(len(body))
	preview := body
	truncated := false
	if maxBytes > 0 && int64(len(preview)) > maxBytes {
		preview = preview[:maxBytes]
		truncated = true
	}

	format := DetectBody(preview)
	processed := ProcessedBody{
		Format:    format,
		Size:      size,
		SHA256:    hex.EncodeToString(sum[:]),
		Truncated: truncated,
	}
	if format == domain.BodyBytes {
		processed.Bytes = append([]byte(nil), preview...)
		return processed
	}

	text := string(preview)
	if redact {
		next, changed := RedactBodyText(format, text)
		text = next
		processed.Redacted = changed
	}
	processed.Text = text
	return processed
}

func DetectBody(body []byte) domain.BodyFormat {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return domain.BodyEmpty
	}
	if !utf8.Valid(trimmed) {
		return domain.BodyBytes
	}
	text := string(trimmed)
	if (strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[")) && json.Valid(trimmed) {
		return domain.BodyJSON
	}
	if strings.HasPrefix(text, "<") {
		var probe any
		if err := xml.Unmarshal(trimmed, &probe); err == nil {
			return domain.BodyXML
		}
	}
	if isPrintable(text) {
		return domain.BodyText
	}
	return domain.BodyUnknown
}

func RedactMap(values map[string]string) (map[string]string, bool) {
	out := make(map[string]string, len(values))
	changed := false
	for key, value := range values {
		if sensitiveKey.MatchString(key) {
			out[key] = redactedValue
			changed = true
			continue
		}
		out[key] = value
	}
	return out, changed
}

func RedactBodyText(format domain.BodyFormat, text string) (string, bool) {
	switch format {
	case domain.BodyJSON:
		var value any
		if err := json.Unmarshal([]byte(text), &value); err != nil {
			return redactPlainText(text)
		}
		changed := redactJSONValue(value)
		if !changed {
			return text, false
		}
		out, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return text, false
		}
		return string(out), true
	case domain.BodyXML, domain.BodyText, domain.BodyUnknown:
		return redactPlainText(text)
	default:
		return text, false
	}
}

func redactJSONValue(value any) bool {
	changed := false
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if sensitiveKey.MatchString(key) {
				typed[key] = redactedValue
				changed = true
				continue
			}
			if redactJSONValue(child) {
				changed = true
			}
		}
	case []any:
		for _, child := range typed {
			if redactJSONValue(child) {
				changed = true
			}
		}
	}
	return changed
}

func redactPlainText(text string) (string, bool) {
	changed := false
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 && sensitiveKey.MatchString(parts[0]) {
			lines[i] = parts[0] + "=" + redactedValue
			changed = true
		}
	}
	return strings.Join(lines, "\n"), changed
}

func isPrintable(text string) bool {
	for _, r := range text {
		if r == '\n' || r == '\r' || r == '\t' {
			continue
		}
		if r < 32 {
			return false
		}
	}
	return true
}
