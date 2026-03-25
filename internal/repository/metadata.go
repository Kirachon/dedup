package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"dedup/internal/model"
)

const auditDetailRedactedValue = "[redacted]"

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

	safeDetailsJSON, err := scrubAuditDetailsJSON(audit.DetailsJSON)
	if err != nil {
		return fmt.Errorf("scrub audit details: %w", err)
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
`, audit.AuditID, audit.EntityType, audit.EntityID, audit.Action, audit.PerformedBy, stringValue(safeDetailsJSON), audit.CreatedAt)
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

func scrubAuditDetailsJSON(detailsJSON *string) (*string, error) {
	if detailsJSON == nil {
		return nil, nil
	}

	trimmed := strings.TrimSpace(*detailsJSON)
	if trimmed == "" {
		return nil, nil
	}

	var payload any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return nil, fmt.Errorf("unmarshal audit details json: %w", err)
	}

	scrubbed := scrubAuditValue("", payload)
	encoded, err := json.Marshal(scrubbed)
	if err != nil {
		return nil, fmt.Errorf("marshal scrubbed audit details json: %w", err)
	}

	value := string(encoded)
	return &value, nil
}

func scrubAuditValue(key string, value any) any {
	switch typed := value.(type) {
	case map[string]any:
		scrubbed := make(map[string]any, len(typed))
		for childKey, childValue := range typed {
			scrubbed[childKey] = scrubAuditValue(childKey, childValue)
		}
		return scrubbed
	case []any:
		scrubbed := make([]any, len(typed))
		for i, childValue := range typed {
			scrubbed[i] = scrubAuditValue(key, childValue)
		}
		return scrubbed
	case string:
		return scrubAuditString(key, typed)
	default:
		return value
	}
}

func scrubAuditString(key, value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	normalizedKey := strings.ToLower(strings.TrimSpace(key))
	switch normalizedKey {
	case "notes", "remarks", "expected_confirmation":
		return auditDetailRedactedValue
	}

	if strings.Contains(normalizedKey, "path") || strings.Contains(normalizedKey, "file") {
		base := portableAuditPathBase(trimmed)
		if base == "" {
			return auditDetailRedactedValue
		}
		return base
	}

	if strings.Contains(normalizedKey, "source_reference") {
		base := portableAuditPathBase(trimmed)
		if base != "" {
			return base
		}
	}

	return trimmed
}

func portableAuditPathBase(value string) string {
	normalized := strings.ReplaceAll(strings.TrimSpace(value), "\\", "/")
	if normalized == "" {
		return ""
	}

	base := path.Base(path.Clean(normalized))
	if base == "." || base == "/" {
		return ""
	}
	return base
}
