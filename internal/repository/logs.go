package repository

import (
	"context"
	"database/sql"
	"fmt"

	"dedup/internal/model"
)

// CreateImportLog inserts a new import log row.
func (r *Repository) CreateImportLog(ctx context.Context, log *model.ImportLog) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createImportLog(background(ctx), log)
	})
}

// UpdateImportLog updates an import log row by import ID.
func (r *Repository) UpdateImportLog(ctx context.Context, log *model.ImportLog) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.updateImportLog(background(ctx), log)
	})
}

// GetImportLog loads an import log by ID.
func (r *Repository) GetImportLog(ctx context.Context, importID string) (*model.ImportLog, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT import_id, source_type, source_reference, file_name, file_hash, idempotency_key, rows_read, rows_inserted, rows_skipped, rows_failed, status, started_at, completed_at, checkpoint_token, operator_name, remarks
FROM import_logs
WHERE import_id = ?
LIMIT 1;
`, importID)

	return scanImportLog(row)
}

// ListImportLogs returns deterministic import history.
func (r *Repository) ListImportLogs(ctx context.Context, query ImportLogListQuery) ([]model.ImportLog, error) {
	if err := normalizeOffset(query.Offset); err != nil {
		return nil, err
	}
	limit := normalizeLimit(query.Limit, defaultGenericLimit)

	builder := sqlBuilder{}
	builder.addEquals("source_type", query.SourceType)
	builder.addEquals("status", query.Status)

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT import_id, source_type, source_reference, file_name, file_hash, idempotency_key, rows_read, rows_inserted, rows_skipped, rows_failed, status, started_at, completed_at, checkpoint_token, operator_name, remarks
FROM import_logs
WHERE `+builder.where()+`
ORDER BY started_at DESC, import_id ASC
LIMIT ? OFFSET ?;
`, append(builder.args, limit, query.Offset)...)
	if err != nil {
		return nil, fmt.Errorf("list import logs: %w", err)
	}
	defer rows.Close()

	items := make([]model.ImportLog, 0)
	for rows.Next() {
		item, err := scanImportLog(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate import logs: %w", err)
	}

	return items, nil
}

// CreateExportLog inserts a new export log row.
func (r *Repository) CreateExportLog(ctx context.Context, log *model.ExportLog) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createExportLog(background(ctx), log)
	})
}

// UpdateExportLog updates an export log row by export ID.
func (r *Repository) UpdateExportLog(ctx context.Context, log *model.ExportLog) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.updateExportLog(background(ctx), log)
	})
}

// GetExportLog loads an export log by ID.
func (r *Repository) GetExportLog(ctx context.Context, exportID string) (*model.ExportLog, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT export_id, file_name, export_type, rows_exported, created_at, performed_by
FROM export_logs
WHERE export_id = ?
LIMIT 1;
`, exportID)

	return scanExportLog(row)
}

// ListExportLogs returns deterministic export history.
func (r *Repository) ListExportLogs(ctx context.Context, query ExportLogListQuery) ([]model.ExportLog, error) {
	if err := normalizeOffset(query.Offset); err != nil {
		return nil, err
	}
	limit := normalizeLimit(query.Limit, defaultGenericLimit)

	builder := sqlBuilder{}
	builder.addEquals("export_type", query.ExportType)

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT export_id, file_name, export_type, rows_exported, created_at, performed_by
FROM export_logs
WHERE `+builder.where()+`
ORDER BY created_at DESC, export_id ASC
LIMIT ? OFFSET ?;
`, append(builder.args, limit, query.Offset)...)
	if err != nil {
		return nil, fmt.Errorf("list export logs: %w", err)
	}
	defer rows.Close()

	items := make([]model.ExportLog, 0)
	for rows.Next() {
		item, err := scanExportLog(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate export logs: %w", err)
	}

	return items, nil
}

func (r *Repository) createImportLog(ctx context.Context, log *model.ImportLog) error {
	if log == nil {
		return fmt.Errorf("import log is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO import_logs (
    import_id,
    source_type,
    source_reference,
    file_name,
    file_hash,
    idempotency_key,
    rows_read,
    rows_inserted,
    rows_skipped,
    rows_failed,
    status,
    started_at,
    completed_at,
    checkpoint_token,
    operator_name,
    remarks
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`, log.ImportID, log.SourceType, log.SourceReference, stringValue(log.FileName), stringValue(log.FileHash), stringValue(log.IdempotencyKey), log.RowsRead, log.RowsInserted, log.RowsSkipped, log.RowsFailed, log.Status, log.StartedAt, stringValue(log.CompletedAt), stringValue(log.CheckpointToken), stringValue(log.OperatorName), stringValue(log.Remarks))
	if err != nil {
		return fmt.Errorf("insert import log: %w", err)
	}

	return nil
}

func (r *Repository) updateImportLog(ctx context.Context, log *model.ImportLog) error {
	if log == nil {
		return fmt.Errorf("import log is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE import_logs SET
    file_name = ?,
    file_hash = ?,
    idempotency_key = ?,
    rows_read = ?,
    rows_inserted = ?,
    rows_skipped = ?,
    rows_failed = ?,
    status = ?,
    completed_at = ?,
    checkpoint_token = ?,
    operator_name = ?,
    remarks = ?
WHERE import_id = ?;
`, stringValue(log.FileName), stringValue(log.FileHash), stringValue(log.IdempotencyKey), log.RowsRead, log.RowsInserted, log.RowsSkipped, log.RowsFailed, log.Status, stringValue(log.CompletedAt), stringValue(log.CheckpointToken), stringValue(log.OperatorName), stringValue(log.Remarks), log.ImportID)
	if err != nil {
		return fmt.Errorf("update import log: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update import log rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Repository) createExportLog(ctx context.Context, log *model.ExportLog) error {
	if log == nil {
		return fmt.Errorf("export log is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO export_logs (
    export_id,
    file_name,
    export_type,
    rows_exported,
    created_at,
    performed_by
) VALUES (?, ?, ?, ?, ?, ?);
`, log.ExportID, log.FileName, log.ExportType, log.RowsExported, log.CreatedAt, stringValue(log.PerformedBy))
	if err != nil {
		return fmt.Errorf("insert export log: %w", err)
	}

	return nil
}

func (r *Repository) updateExportLog(ctx context.Context, log *model.ExportLog) error {
	if log == nil {
		return fmt.Errorf("export log is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE export_logs SET
    file_name = ?,
    export_type = ?,
    rows_exported = ?,
    performed_by = ?
WHERE export_id = ?;
`, log.FileName, log.ExportType, log.RowsExported, stringValue(log.PerformedBy), log.ExportID)
	if err != nil {
		return fmt.Errorf("update export log: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update export log rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func scanImportLog(scanner rowScanner) (*model.ImportLog, error) {
	var (
		item            model.ImportLog
		fileName        sql.NullString
		fileHash        sql.NullString
		idempotencyKey  sql.NullString
		completedAt     sql.NullString
		checkpointToken sql.NullString
		operatorName    sql.NullString
		remarks         sql.NullString
	)

	if err := scanner.Scan(&item.ImportID, &item.SourceType, &item.SourceReference, &fileName, &fileHash, &idempotencyKey, &item.RowsRead, &item.RowsInserted, &item.RowsSkipped, &item.RowsFailed, &item.Status, &item.StartedAt, &completedAt, &checkpointToken, &operatorName, &remarks); err != nil {
		return nil, err
	}

	item.FileName = stringPtrFromNullString(fileName)
	item.FileHash = stringPtrFromNullString(fileHash)
	item.IdempotencyKey = stringPtrFromNullString(idempotencyKey)
	item.CompletedAt = stringPtrFromNullString(completedAt)
	item.CheckpointToken = stringPtrFromNullString(checkpointToken)
	item.OperatorName = stringPtrFromNullString(operatorName)
	item.Remarks = stringPtrFromNullString(remarks)
	return &item, nil
}

func scanExportLog(scanner rowScanner) (*model.ExportLog, error) {
	var (
		item        model.ExportLog
		performedBy sql.NullString
	)

	if err := scanner.Scan(&item.ExportID, &item.FileName, &item.ExportType, &item.RowsExported, &item.CreatedAt, &performedBy); err != nil {
		return nil, err
	}

	item.PerformedBy = stringPtrFromNullString(performedBy)
	return &item, nil
}
