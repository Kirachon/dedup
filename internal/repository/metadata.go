package repository

import (
	"context"
	"database/sql"
	"fmt"

	"dedup/internal/model"
)

// UpsertSetting inserts or updates a single application setting.
func (r *Repository) UpsertSetting(ctx context.Context, setting *model.AppSetting) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.upsertSetting(background(ctx), setting)
	})
}

// GetSetting loads one application setting by key.
func (r *Repository) GetSetting(ctx context.Context, key string) (*model.AppSetting, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT setting_key, setting_value, updated_at
FROM app_settings
WHERE setting_key = ?
LIMIT 1;
`, key)

	return scanSetting(row)
}

// ListSettings returns all settings in deterministic key order.
func (r *Repository) ListSettings(ctx context.Context) ([]model.AppSetting, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT setting_key, setting_value, updated_at
FROM app_settings
ORDER BY setting_key ASC;
`)
	if err != nil {
		return nil, fmt.Errorf("list settings: %w", err)
	}
	defer rows.Close()

	items := make([]model.AppSetting, 0)
	for rows.Next() {
		item, err := scanSetting(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate settings: %w", err)
	}

	return items, nil
}

// CreateAuditLog inserts a new immutable audit log row.
func (r *Repository) CreateAuditLog(ctx context.Context, audit *model.AuditLog) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createAuditLog(background(ctx), audit)
	})
}

// GetAuditLog loads a single audit log by ID.
func (r *Repository) GetAuditLog(ctx context.Context, auditID string) (*model.AuditLog, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT audit_id, entity_type, entity_id, action, performed_by, details_json, created_at
FROM audit_logs
WHERE audit_id = ?
LIMIT 1;
`, auditID)

	return scanAuditLog(row)
}

// ListAuditLogs returns immutable audit rows with optional filters.
func (r *Repository) ListAuditLogs(ctx context.Context, query AuditLogQuery) ([]model.AuditLog, error) {
	if err := normalizeOffset(query.Offset); err != nil {
		return nil, err
	}
	limit := normalizeLimit(query.Limit, defaultGenericLimit)

	builder := sqlBuilder{}
	builder.addEquals("entity_type", query.EntityType)
	builder.addEquals("entity_id", query.EntityID)
	builder.addEquals("action", query.Action)
	builder.addEquals("performed_by", query.PerformedBy)

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT audit_id, entity_type, entity_id, action, performed_by, details_json, created_at
FROM audit_logs
WHERE `+builder.where()+`
ORDER BY created_at DESC, audit_id ASC
LIMIT ? OFFSET ?;
`, append(builder.args, limit, query.Offset)...)
	if err != nil {
		return nil, fmt.Errorf("list audit logs: %w", err)
	}
	defer rows.Close()

	items := make([]model.AuditLog, 0)
	for rows.Next() {
		item, err := scanAuditLog(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit logs: %w", err)
	}

	return items, nil
}

func (r *Repository) upsertSetting(ctx context.Context, setting *model.AppSetting) error {
	if setting == nil {
		return fmt.Errorf("setting is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO app_settings (setting_key, setting_value, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(setting_key) DO UPDATE SET
    setting_value = excluded.setting_value,
    updated_at = excluded.updated_at;
`, setting.SettingKey, setting.SettingValue, setting.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert setting: %w", err)
	}

	return nil
}

func (r *Repository) createAuditLog(ctx context.Context, audit *model.AuditLog) error {
	if audit == nil {
		return fmt.Errorf("audit log is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO audit_logs (
    audit_id,
    entity_type,
    entity_id,
    action,
    performed_by,
    details_json,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?);
`, audit.AuditID, audit.EntityType, audit.EntityID, audit.Action, audit.PerformedBy, stringValue(audit.DetailsJSON), audit.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert audit log: %w", err)
	}

	return nil
}

func scanSetting(scanner rowScanner) (*model.AppSetting, error) {
	var item model.AppSetting
	if err := scanner.Scan(&item.SettingKey, &item.SettingValue, &item.UpdatedAt); err != nil {
		return nil, err
	}
	return &item, nil
}

func scanAuditLog(scanner rowScanner) (*model.AuditLog, error) {
	var (
		item        model.AuditLog
		detailsJSON sql.NullString
	)

	if err := scanner.Scan(&item.AuditID, &item.EntityType, &item.EntityID, &item.Action, &item.PerformedBy, &detailsJSON, &item.CreatedAt); err != nil {
		return nil, err
	}

	item.DetailsJSON = stringPtrFromNullString(detailsJSON)
	return &item, nil
}
