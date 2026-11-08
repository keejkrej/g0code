package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

func (s *Store) ClaimEffects(ctx context.Context, workerID string, limit int, lease time.Duration) ([]orchestration.OutboxEffect, error) {
	if workerID == "" {
		return nil, fmt.Errorf("worker ID is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if lease <= 0 {
		lease = time.Minute
	}
	now := time.Now().UTC()
	leaseExpires := now.Add(lease)
	claimed := make([]orchestration.OutboxEffect, 0, limit)
	err := s.transaction(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, `SELECT payload_json FROM orchestration_effects
			WHERE status = 'pending' OR (status = 'running' AND lease_expires_at <= ?)
			ORDER BY created_at, id LIMIT ?`, now.UnixMilli(), limit)
		if err != nil {
			return err
		}
		var effects []orchestration.Effect
		for rows.Next() {
			var data string
			if err := rows.Scan(&data); err != nil {
				rows.Close()
				return err
			}
			var effect orchestration.Effect
			if err := json.Unmarshal([]byte(data), &effect); err != nil {
				rows.Close()
				return err
			}
			effects = append(effects, effect)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, effect := range effects {
			result, err := q.ExecContext(ctx, `UPDATE orchestration_effects
				SET status = 'running', attempts = attempts + 1, claimed_by = ?, lease_expires_at = ?, updated_at = ?
				WHERE id = ? AND (status = 'pending' OR (status = 'running' AND lease_expires_at <= ?))`,
				workerID, leaseExpires.UnixMilli(), now.Format(time.RFC3339Nano), effect.ID, now.UnixMilli())
			if err != nil {
				return err
			}
			count, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if count != 1 {
				continue
			}
			claimed = append(claimed, orchestration.OutboxEffect{Effect: effect, Status: orchestration.EffectRunning,
				ClaimedBy: workerID, LeaseExpiresAt: &leaseExpires, UpdatedAt: now})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Read durable attempts for callers/tests while preserving claim order.
	for index := range claimed {
		row := s.db.QueryRowContext(ctx, "SELECT attempts FROM orchestration_effects WHERE id = ?", claimed[index].Effect.ID)
		if err := row.Scan(&claimed[index].Attempts); err != nil {
			return nil, err
		}
	}
	return claimed, nil
}

func (s *Store) CompleteEffect(ctx context.Context, id, workerID string, executionErr error) error {
	status := orchestration.EffectCompleted
	var failure *string
	if executionErr != nil {
		status = orchestration.EffectFailed
		message := executionErr.Error()
		failure = &message
	}
	result, err := s.db.ExecContext(ctx, `UPDATE orchestration_effects
		SET status = ?, last_error = ?, claimed_by = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE id = ? AND claimed_by = ? AND status = 'running' AND lease_expires_at > ?`,
		status, failure, time.Now().UTC().Format(time.RFC3339Nano), id, workerID, time.Now().UTC().UnixMilli())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("effect %s is no longer claimed by %s", id, workerID)
	}
	return nil
}

func (s *Store) ListEffects(ctx context.Context) ([]orchestration.OutboxEffect, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload_json, status, attempts, last_error, claimed_by,
		lease_expires_at, created_at, updated_at FROM orchestration_effects ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]orchestration.OutboxEffect, 0)
	for rows.Next() {
		var row orcffectRow
		if err := rows.Scan(&row.Data, &row.Status, &row.Attempts, &row.LastError, &row.ClaimedBy,
			&row.LeaseExpiresAt, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		effect, err := row.decode()
		if err != nil {
			return nil, err
		}
		result = append(result, effect)
	}
	return result, rows.Err()
}

type orcffectRow struct {
	Data           string
	Status         orchestration.EffectStatus
	Attempts       int
	LastError      *string
	ClaimedBy      *string
	LeaseExpiresAt *int64
	CreatedAt      string
	UpdatedAt      string
}

func (row orcffectRow) decode() (orchestration.OutboxEffect, error) {
	var effect orchestration.Effect
	if err := json.Unmarshal([]byte(row.Data), &effect); err != nil {
		return orchestration.OutboxEffect{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, row.CreatedAt)
	if err != nil {
		return orchestration.OutboxEffect{}, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, row.UpdatedAt)
	if err != nil {
		return orchestration.OutboxEffect{}, err
	}
	result := orchestration.OutboxEffect{Effect: effect, Status: row.Status, Attempts: row.Attempts,
		LastError: row.LastError, CreatedAt: createdAt, UpdatedAt: updatedAt}
	if row.ClaimedBy != nil {
		result.ClaimedBy = *row.ClaimedBy
	}
	if row.LeaseExpiresAt != nil {
		expires := time.UnixMilli(*row.LeaseExpiresAt).UTC()
		result.LeaseExpiresAt = &expires
	}
	return result, nil
}

var _ orchestration.OutboxStore = (*Store)(nil)
