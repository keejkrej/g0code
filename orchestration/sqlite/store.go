// Package sqlite provides durable orchestration events, receipts, and effects.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/keejkrej/g0code/orchestration"
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	dsn := path
	if path != ":memory:" {
		dsn += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)&_pragma=foreign_keys(1)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// A single connection also makes :memory: deterministic. BEGIN IMMEDIATE
	// serializes command writers and makes sequence checks transactional.
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	schema := `
CREATE TABLE IF NOT EXISTS orchestration_events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  thread_id TEXT NOT NULL,
  command_id TEXT,
  event_type TEXT NOT NULL,
  raw_event_id TEXT,
  event_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_events_thread ON orchestration_events(thread_id, sequence);
CREATE INDEX IF NOT EXISTS orchestration_events_command ON orchestration_events(command_id);
CREATE INDEX IF NOT EXISTS orchestration_events_raw ON orchestration_events(raw_event_id);
CREATE TABLE IF NOT EXISTS orchestration_receipts (
  command_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  command_type TEXT NOT NULL,
  accepted_at TEXT NOT NULL,
  result_sequence INTEGER NOT NULL,
  status TEXT NOT NULL,
  error TEXT
);
CREATE TABLE IF NOT EXISTS orchestration_effects (
  id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  claimed_by TEXT,
  lease_expires_at INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_effects_claim ON orchestration_effects(status, lease_expires_at, created_at);
`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *Store) transaction(ctx context.Context, fn func(querier) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	if err := fn(conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return err
}

func (s *Store) Snapshot(ctx context.Context, threadID orchestration.ThreadID) (orchestration.ThreadSnapshot, error) {
	events, err := s.Read(ctx, orchestration.EventFilter{ThreadID: &threadID})
	if err != nil {
		return orchestration.ThreadSnapshot{}, err
	}
	if len(events) == 0 {
		return orchestration.ThreadSnapshot{}, orchestration.ErrThreadNotFound
	}
	projections, err := orchestration.Replay(events)
	if err != nil {
		return orchestration.ThreadSnapshot{}, err
	}
	projection, ok := projections[threadID]
	if !ok {
		return orchestration.ThreadSnapshot{}, orchestration.ErrThreadNotFound
	}
	return orchestration.ThreadSnapshot{Projection: projection, SnapshotSequence: events[len(events)-1].Sequence}, nil
}

func (s *Store) List(ctx context.Context) ([]orchestration.ThreadSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT DISTINCT thread_id FROM orchestration_events ORDER BY thread_id")
	if err != nil {
		return nil, err
	}
	var ids []orchestration.ThreadID
	for rows.Next() {
		var id orchestration.ThreadID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]orchestration.ThreadSnapshot, 0, len(ids))
	for _, id := range ids {
		snapshot, err := s.Snapshot(ctx, id)
		if err != nil {
			return nil, err
		}
		result = append(result, snapshot)
	}
	return result, nil
}

func (s *Store) Read(ctx context.Context, filter orchestration.EventFilter) ([]orchestration.StoredEvent, error) {
	query := "SELECT event_json FROM orchestration_events"
	conditions := []string{}
	args := []any{}
	if filter.AfterSequence != nil {
		conditions = append(conditions, "sequence > ?")
		args = append(args, *filter.AfterSequence)
	}
	if filter.ThroughSequence != nil {
		conditions = append(conditions, "sequence <= ?")
		args = append(args, *filter.ThroughSequence)
	}
	if filter.ThreadID != nil {
		conditions = append(conditions, "thread_id = ?")
		args = append(args, *filter.ThreadID)
	}
	if filter.EventType != nil {
		conditions = append(conditions, "event_type = ?")
		args = append(args, *filter.EventType)
	}
	if filter.CommandID != nil {
		conditions = append(conditions, "command_id = ?")
		args = append(args, *filter.CommandID)
	}
	if filter.RawEventID != nil {
		conditions = append(conditions, "raw_event_id = ?")
		args = append(args, *filter.RawEventID)
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY sequence"
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]orchestration.StoredEvent, 0)
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var event orchestration.StoredEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *Store) GetReceipt(ctx context.Context, commandID orchestration.CommandID) (orchestration.Receipt, bool, error) {
	return readReceipt(ctx, s.db, commandID)
}

func readReceipt(ctx context.Context, q querier, commandID orchestration.CommandID) (orchestration.Receipt, bool, error) {
	row := q.QueryRowContext(ctx, "SELECT command_id, thread_id, command_type, accepted_at, result_sequence, status, error FROM orchestration_receipts WHERE command_id = ?", commandID)
	var receipt orchestration.Receipt
	var acceptedAt string
	var failure *string
	if err := row.Scan(&receipt.CommandID, &receipt.ThreadID, &receipt.CommandType, &acceptedAt,
		&receipt.ResultSequence, &receipt.Status, &failure); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return orchestration.Receipt{}, false, nil
		}
		return orchestration.Receipt{}, false, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, acceptedAt)
	if err != nil {
		return orchestration.Receipt{}, false, err
	}
	receipt.AcceptedAt = parsed
	receipt.Error = failure
	return receipt, true, nil
}

func (s *Store) Commit(ctx context.Context, input orchestration.CommitInput) (orchestration.CommitResult, error) {
	var result orchestration.CommitResult
	err := s.transaction(ctx, func(q querier) error {
		if input.Receipt != nil {
			existing, ok, err := readReceipt(ctx, q, input.Receipt.CommandID)
			if err != nil {
				return err
			}
			if ok {
				events, err := readEvents(ctx, q, orchestration.EventFilter{CommandID: &input.Receipt.CommandID})
				result = orchestration.CommitResult{Sequence: existing.ResultSequence, StoredEvents: events, Receipt: &existing, Committed: false}
				return err
			}
		}
		if input.Receipt == nil || input.Receipt.Status == orchestration.ReceiptAccepted {
			expected := map[orchestration.ThreadID]uint64{input.ThreadID: input.ExpectedSequence}
			for threadID, sequence := range input.ExpectedSequences {
				expected[threadID] = sequence
			}
			for threadID, sequence := range expected {
				var current uint64
				if err := q.QueryRowContext(ctx, "SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events WHERE thread_id = ?", threadID).Scan(&current); err != nil {
					return err
				}
				if current != sequence {
					return fmt.Errorf("%w for thread %s: expected %d, actual %d", orchestration.ErrSequenceConflict, threadID, sequence, current)
				}
			}
		}
		resultSequence := input.ExpectedSequence
		stagedProjections := make(map[orchestration.ThreadID]orchestration.Projection)
		for _, event := range input.Events {
			data, err := json.Marshal(event)
			if err != nil {
				return err
			}
			projection, loaded := stagedProjections[event.ThreadID]
			if !loaded {
				previous, err := readThreadEvents(ctx, q, event.ThreadID)
				if err != nil {
					return err
				}
				if len(previous) > 0 {
					projections, err := orchestration.Replay(previous)
					if err != nil {
						return err
					}
					projection = projections[event.ThreadID]
				}
			}
			updatedProjection, err := orchestration.Apply(projection, event)
			if err != nil {
				return err
			}
			stagedProjections[event.ThreadID] = updatedProjection
			var commandID *orchestration.CommandID
			if input.Receipt != nil {
				commandID = &input.Receipt.CommandID
			} else {
				commandID = input.CommandID
			}
			insert, err := q.ExecContext(ctx, `INSERT INTO orchestration_events
				(event_id, thread_id, command_id, event_type, raw_event_id, event_json) VALUES (?, ?, ?, ?, ?, ?)`,
				event.ID, event.ThreadID, commandID, event.Type, event.RawEventID, string(data))
			if err != nil {
				return err
			}
			sequence, err := insert.LastInsertId()
			if err != nil {
				return err
			}
			eventJSON, _ := json.Marshal(event)
			stored := orchestration.StoredEvent{Sequence: uint64(sequence), CommandID: commandID}
			if err := json.Unmarshal(eventJSON, &stored.Event); err != nil {
				return err
			}
			storedJSON, err := json.Marshal(stored)
			if err != nil {
				return err
			}
			if _, err := q.ExecContext(ctx, "UPDATE orchestration_events SET event_json = ? WHERE sequence = ?", string(storedJSON), sequence); err != nil {
				return err
			}
			result.StoredEvents = append(result.StoredEvents, stored)
			resultSequence = uint64(sequence)
		}
		createdAt := time.Now().UTC()
		if input.Receipt != nil {
			createdAt = input.Receipt.AcceptedAt
		}
		for _, effect := range input.Effects {
			data, err := json.Marshal(effect)
			if err != nil {
				return err
			}
			if _, err := q.ExecContext(ctx, `INSERT INTO orchestration_effects
				(id, command_id, thread_id, kind, payload_json, status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`, effect.ID, effect.CommandID, effect.ThreadID, effect.Kind,
				string(data), createdAt.Format(time.RFC3339Nano), createdAt.Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
		result.Sequence = resultSequence
		result.Committed = true
		if input.Receipt != nil {
			receipt := *input.Receipt
			receipt.ResultSequence = resultSequence
			if _, err := q.ExecContext(ctx, `INSERT INTO orchestration_receipts
				(command_id, thread_id, command_type, accepted_at, result_sequence, status, error)
				VALUES (?, ?, ?, ?, ?, ?, ?)`, receipt.CommandID, receipt.ThreadID, receipt.CommandType,
				receipt.AcceptedAt.Format(time.RFC3339Nano), receipt.ResultSequence, receipt.Status, receipt.Error); err != nil {
				return err
			}
			result.Receipt = &receipt
		}
		return nil
	})
	return result, err
}

func readThreadEvents(ctx context.Context, q querier, threadID orchestration.ThreadID) ([]orchestration.StoredEvent, error) {
	rows, err := q.QueryContext(ctx, "SELECT event_json FROM orchestration_events WHERE thread_id = ? ORDER BY sequence", threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []orchestration.StoredEvent
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var event orchestration.StoredEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func readEvents(ctx context.Context, q querier, filter orchestration.EventFilter) ([]orchestration.StoredEvent, error) {
	// Receipt replay reads a tiny command batch, so reuse the explicit command predicate.
	rows, err := q.QueryContext(ctx, "SELECT event_json FROM orchestration_events WHERE command_id = ? ORDER BY sequence", filter.CommandID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []orchestration.StoredEvent
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var event orchestration.StoredEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}
