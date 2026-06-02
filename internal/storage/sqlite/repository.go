package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmanzano/mq-lens/internal/domain"
	_ "modernc.org/sqlite"
)

type Repository struct {
	db *sql.DB
}

func Open(path string) (*Repository, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, err
	}
	repo := &Repository{db: db}
	if err := repo.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return repo, nil
}

func (r *Repository) Close() error {
	return r.db.Close()
}

func (r *Repository) migrate(ctx context.Context) error {
	statements := []string{
		`PRAGMA foreign_keys = ON`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
		`CREATE TABLE IF NOT EXISTS captured_messages (
			id TEXT PRIMARY KEY,
			captured_at TEXT NOT NULL,
			broker TEXT NOT NULL,
			original_destination TEXT NOT NULL,
			audit_destination TEXT NOT NULL,
			destination_type TEXT NOT NULL,
			message_id TEXT,
			correlation_id TEXT,
			reply_to TEXT,
			message_type TEXT,
			persistent INTEGER,
			priority INTEGER,
			timestamp TEXT,
			expiration TEXT,
			body_format TEXT NOT NULL,
			body_text TEXT,
			body_bytes BLOB,
			body_size INTEGER NOT NULL,
			body_sha256 TEXT NOT NULL,
			truncated INTEGER NOT NULL,
			redacted INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS message_headers (
			message_id TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT,
			PRIMARY KEY (message_id, key),
			FOREIGN KEY (message_id) REFERENCES captured_messages(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS message_properties (
			message_id TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT,
			PRIMARY KEY (message_id, key),
			FOREIGN KEY (message_id) REFERENCES captured_messages(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS topology_events (
			id TEXT PRIMARY KEY,
			event_at TEXT NOT NULL,
			event_type TEXT NOT NULL,
			destination_type TEXT,
			destination_name TEXT,
			client_id TEXT,
			connection_id TEXT,
			producer_id TEXT,
			consumer_id TEXT,
			raw_json TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS message_notes (
			id TEXT PRIMARY KEY,
			message_id TEXT NOT NULL,
			note TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY (message_id) REFERENCES captured_messages(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_captured_at ON captured_messages(captured_at)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_original_destination ON captured_messages(original_destination)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_correlation_id ON captured_messages(correlation_id)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_jms_message_id ON captured_messages(message_id)`,
		`CREATE INDEX IF NOT EXISTS idx_headers_key_value ON message_headers(key, value)`,
		`CREATE INDEX IF NOT EXISTS idx_properties_key_value ON message_properties(key, value)`,
	}
	for _, statement := range statements {
		if _, err := r.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func sqliteDSN(path string) string {
	values := url.Values{}
	values.Set("_pragma", "foreign_keys(1)")
	values.Add("_pragma", "busy_timeout(5000)")
	return path + "?" + values.Encode()
}

func (r *Repository) SaveMessage(ctx context.Context, message domain.CapturedMessage) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO captured_messages (
		id, captured_at, broker, original_destination, audit_destination, destination_type,
		message_id, correlation_id, reply_to, message_type, persistent, priority, timestamp, expiration,
		body_format, body_text, body_bytes, body_size, body_sha256, truncated, redacted
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		message.ID, message.CapturedAt.Format(time.RFC3339Nano), message.Broker, message.OriginalDestination,
		message.AuditDestination, string(message.DestinationType), message.MessageID, message.CorrelationID,
		message.ReplyTo, message.Type, boolInt(message.Persistent), message.Priority, nullableTime(message.Timestamp),
		nullableTime(message.Expiration), string(message.BodyFormat), message.BodyText, message.BodyBytes,
		message.BodySize, message.BodySHA256, boolInt(message.Truncated), boolInt(message.Redacted),
	)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM message_headers WHERE message_id = ?`, message.ID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM message_properties WHERE message_id = ?`, message.ID); err != nil {
		return err
	}
	for key, value := range message.Headers {
		if _, err = tx.ExecContext(ctx, `INSERT INTO message_headers (message_id, key, value) VALUES (?, ?, ?)`, message.ID, key, value); err != nil {
			return err
		}
	}
	for key, value := range message.Properties {
		if _, err = tx.ExecContext(ctx, `INSERT INTO message_properties (message_id, key, value) VALUES (?, ?, ?)`, message.ID, key, value); err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

func (r *Repository) ListMessages(ctx context.Context, filter domain.MessageFilter) ([]domain.CapturedMessage, error) {
	if filter.Limit <= 0 || filter.Limit > 500 {
		filter.Limit = 100
	}
	var args []any
	where := []string{"1=1"}
	if filter.Destination != "" {
		where = append(where, "original_destination = ?")
		args = append(args, filter.Destination)
	}
	if filter.DestinationType != "" {
		where = append(where, "destination_type = ?")
		args = append(args, filter.DestinationType)
	}
	if filter.CorrelationID != "" {
		where = append(where, "correlation_id = ?")
		args = append(args, filter.CorrelationID)
	}
	if filter.MessageID != "" {
		where = append(where, "message_id = ?")
		args = append(args, filter.MessageID)
	}
	if filter.Contains != "" {
		where = append(where, "body_text LIKE ?")
		args = append(args, "%"+filter.Contains+"%")
	}
	if filter.HeaderKey != "" {
		where = append(where, "EXISTS (SELECT 1 FROM message_headers h WHERE h.message_id = captured_messages.id AND h.key = ? AND (? = '' OR h.value = ?))")
		args = append(args, filter.HeaderKey, filter.HeaderValue, filter.HeaderValue)
	}
	if filter.PropertyKey != "" {
		where = append(where, "EXISTS (SELECT 1 FROM message_properties p WHERE p.message_id = captured_messages.id AND p.key = ? AND (? = '' OR p.value = ?))")
		args = append(args, filter.PropertyKey, filter.PropertyValue, filter.PropertyValue)
	}
	order := "DESC"
	if filter.SortAsc {
		order = "ASC"
	}
	query := `SELECT id, captured_at, broker, original_destination, audit_destination, destination_type,
		message_id, correlation_id, reply_to, message_type, persistent, priority, timestamp, expiration,
		body_format, body_text, body_bytes, body_size, body_sha256, truncated, redacted
		FROM captured_messages WHERE ` + strings.Join(where, " AND ") + ` ORDER BY captured_at ` + order + ` LIMIT ? OFFSET ?`
	args = append(args, filter.Limit, filter.Offset)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var messages []domain.CapturedMessage
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range messages {
		if err := r.fillMaps(ctx, &messages[i]); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func (r *Repository) GetMessage(ctx context.Context, id string) (domain.CapturedMessage, error) {
	row := r.db.QueryRowContext(ctx, `SELECT id, captured_at, broker, original_destination, audit_destination, destination_type,
		message_id, correlation_id, reply_to, message_type, persistent, priority, timestamp, expiration,
		body_format, body_text, body_bytes, body_size, body_sha256, truncated, redacted
		FROM captured_messages WHERE id = ?`, id)
	message, err := scanMessage(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.CapturedMessage{}, err
		}
		return domain.CapturedMessage{}, err
	}
	if err := r.fillMaps(ctx, &message); err != nil {
		return domain.CapturedMessage{}, err
	}
	return message, nil
}

func (r *Repository) CountMessages(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM captured_messages`).Scan(&count)
	return count, err
}

func (r *Repository) AddNote(ctx context.Context, messageID, note string) (domain.MessageNote, error) {
	item := domain.MessageNote{ID: uuid.NewString(), MessageID: messageID, Note: note, CreatedAt: time.Now().UTC()}
	_, err := r.db.ExecContext(ctx, `INSERT INTO message_notes (id, message_id, note, created_at) VALUES (?, ?, ?, ?)`,
		item.ID, item.MessageID, item.Note, item.CreatedAt.Format(time.RFC3339Nano))
	return item, err
}

func (r *Repository) DeleteNote(ctx context.Context, noteID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM message_notes WHERE id = ?`, noteID)
	return err
}

func (r *Repository) Cleanup(ctx context.Context, retentionHours, maxMessages int) error {
	if retentionHours > 0 {
		cutoff := time.Now().UTC().Add(-time.Duration(retentionHours) * time.Hour).Format(time.RFC3339Nano)
		if _, err := r.db.ExecContext(ctx, `DELETE FROM captured_messages WHERE captured_at < ?`, cutoff); err != nil {
			return err
		}
	}
	if maxMessages > 0 {
		_, err := r.db.ExecContext(ctx, `DELETE FROM captured_messages WHERE id IN (
			SELECT id FROM captured_messages ORDER BY captured_at DESC LIMIT -1 OFFSET ?
		)`, maxMessages)
		return err
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanMessage(row scanner) (domain.CapturedMessage, error) {
	var message domain.CapturedMessage
	var capturedAt, timestamp, expiration sql.NullString
	var destinationType, bodyFormat string
	var persistent, truncated, redacted int
	err := row.Scan(&message.ID, &capturedAt, &message.Broker, &message.OriginalDestination,
		&message.AuditDestination, &destinationType, &message.MessageID, &message.CorrelationID,
		&message.ReplyTo, &message.Type, &persistent, &message.Priority, &timestamp, &expiration,
		&bodyFormat, &message.BodyText, &message.BodyBytes, &message.BodySize, &message.BodySHA256,
		&truncated, &redacted)
	if err != nil {
		return domain.CapturedMessage{}, err
	}
	if capturedAt.Valid {
		message.CapturedAt, _ = time.Parse(time.RFC3339Nano, capturedAt.String)
	}
	message.Timestamp = parseOptionalTime(timestamp)
	message.Expiration = parseOptionalTime(expiration)
	message.DestinationType = domain.DestinationType(destinationType)
	message.BodyFormat = domain.BodyFormat(bodyFormat)
	message.Persistent = persistent == 1
	message.Truncated = truncated == 1
	message.Redacted = redacted == 1
	message.Headers = map[string]string{}
	message.Properties = map[string]string{}
	return message, nil
}

func (r *Repository) fillMaps(ctx context.Context, message *domain.CapturedMessage) error {
	headers, err := r.loadKeyValues(ctx, "message_headers", message.ID)
	if err != nil {
		return err
	}
	properties, err := r.loadKeyValues(ctx, "message_properties", message.ID)
	if err != nil {
		return err
	}
	message.Headers = headers
	message.Properties = properties
	return nil
}

func (r *Repository) loadKeyValues(ctx context.Context, table, messageID string) (map[string]string, error) {
	query := fmt.Sprintf(`SELECT key, value FROM %s WHERE message_id = ? ORDER BY key`, table)
	rows, err := r.db.QueryContext(ctx, query, messageID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	values := map[string]string{}
	for rows.Next() {
		var key string
		var value sql.NullString
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		values[key] = value.String
	}
	return values, rows.Err()
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.Format(time.RFC3339Nano)
}

func parseOptionalTime(value sql.NullString) *time.Time {
	if !value.Valid || value.String == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value.String)
	if err != nil {
		return nil
	}
	return &parsed
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
