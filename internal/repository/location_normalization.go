package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"dedup/internal/model"
)

// CreateLocationNormalizationRun inserts one normalization run row.
func (r *Repository) CreateLocationNormalizationRun(ctx context.Context, run *model.LocationNormalizationRun) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createLocationNormalizationRun(background(ctx), run)
	})
}

// UpdateLocationNormalizationRun updates mutable run fields by run ID.
func (r *Repository) UpdateLocationNormalizationRun(ctx context.Context, run *model.LocationNormalizationRun) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.updateLocationNormalizationRun(background(ctx), run)
	})
}

// GetLocationNormalizationRun loads one run by ID.
func (r *Repository) GetLocationNormalizationRun(ctx context.Context, runID string) (*model.LocationNormalizationRun, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT run_id, import_id, source_reference, mode, status, normalization_version, total_rows, auto_applied_rows, review_rows, failed_rows, started_at, completed_at
FROM location_normalization_runs
WHERE run_id = ?
LIMIT 1;
`, runID)

	return scanLocationNormalizationRun(row)
}

// ListLocationNormalizationRuns returns deterministic normalization run history.
func (r *Repository) ListLocationNormalizationRuns(ctx context.Context, query LocationNormalizationRunListQuery) ([]model.LocationNormalizationRun, error) {
	if err := normalizeOffset(query.Offset); err != nil {
		return nil, err
	}
	limit := normalizeLimit(query.Limit, defaultGenericLimit)

	builder := sqlBuilder{}
	builder.addEquals("import_id", query.ImportID)
	builder.addEquals("status", query.Status)
	builder.addEquals("mode", query.Mode)

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT run_id, import_id, source_reference, mode, status, normalization_version, total_rows, auto_applied_rows, review_rows, failed_rows, started_at, completed_at
FROM location_normalization_runs
WHERE `+builder.where()+`
ORDER BY started_at DESC, run_id ASC
LIMIT ? OFFSET ?;
`, append(builder.args, limit, query.Offset)...)
	if err != nil {
		return nil, fmt.Errorf("list location normalization runs: %w", err)
	}
	defer rows.Close()

	items := make([]model.LocationNormalizationRun, 0)
	for rows.Next() {
		item, scanErr := scanLocationNormalizationRun(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate location normalization runs: %w", err)
	}

	return items, nil
}

// CreateLocationNormalizationItem inserts one row-level lineage item.
func (r *Repository) CreateLocationNormalizationItem(ctx context.Context, item *model.LocationNormalizationItem) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createLocationNormalizationItem(background(ctx), item)
	})
}

// GetLocationNormalizationItem loads one row-level lineage item by ID.
func (r *Repository) GetLocationNormalizationItem(ctx context.Context, itemID string) (*model.LocationNormalizationItem, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT item_id, run_id, row_number, source_reference, raw_region, raw_province, raw_city, raw_barangay, resolved_region_code, resolved_region_name, resolved_province_code, resolved_province_name, resolved_city_code, resolved_city_name, resolved_barangay_code, resolved_barangay_name, confidence, match_source, status, needs_review, reason, normalization_version, created_at
FROM location_normalization_items
WHERE item_id = ?
LIMIT 1;
`, itemID)

	return scanLocationNormalizationItem(row)
}

// ListLocationNormalizationItems returns deterministic lineage rows.
func (r *Repository) ListLocationNormalizationItems(ctx context.Context, query LocationNormalizationItemListQuery) ([]model.LocationNormalizationItem, error) {
	if err := normalizeOffset(query.Offset); err != nil {
		return nil, err
	}
	limit := normalizeLimit(query.Limit, defaultGenericLimit)

	builder := sqlBuilder{}
	builder.addEquals("run_id", query.RunID)
	builder.addEquals("status", query.Status)
	if query.NeedsReview != nil {
		builder.add("needs_review = ?", boolPtrValue(query.NeedsReview))
	}

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT item_id, run_id, row_number, source_reference, raw_region, raw_province, raw_city, raw_barangay, resolved_region_code, resolved_region_name, resolved_province_code, resolved_province_name, resolved_city_code, resolved_city_name, resolved_barangay_code, resolved_barangay_name, confidence, match_source, status, needs_review, reason, normalization_version, created_at
FROM location_normalization_items
WHERE `+builder.where()+`
ORDER BY row_number ASC, item_id ASC
LIMIT ? OFFSET ?;
`, append(builder.args, limit, query.Offset)...)
	if err != nil {
		return nil, fmt.Errorf("list location normalization items: %w", err)
	}
	defer rows.Close()

	items := make([]model.LocationNormalizationItem, 0)
	for rows.Next() {
		item, scanErr := scanLocationNormalizationItem(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate location normalization items: %w", err)
	}

	return items, nil
}

func (r *Repository) createLocationNormalizationRun(ctx context.Context, run *model.LocationNormalizationRun) error {
	if run == nil {
		return fmt.Errorf("location normalization run is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO location_normalization_runs (
    run_id,
    import_id,
    source_reference,
    mode,
    status,
    normalization_version,
    total_rows,
    auto_applied_rows,
    review_rows,
    failed_rows,
    started_at,
    completed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`, run.RunID, nullableStringValue(ptrToString(run.ImportID)), nullableStringValue(ptrToString(run.SourceReference)), run.Mode, run.Status, strings.TrimSpace(run.NormalizationVersion), run.TotalRows, run.AutoAppliedRows, run.ReviewRows, run.FailedRows, run.StartedAt, nullableStringValue(ptrToString(run.CompletedAt)))
	if err != nil {
		return fmt.Errorf("insert location normalization run: %w", err)
	}

	return nil
}

func (r *Repository) updateLocationNormalizationRun(ctx context.Context, run *model.LocationNormalizationRun) error {
	if run == nil {
		return fmt.Errorf("location normalization run is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE location_normalization_runs SET
    import_id = ?,
    source_reference = ?,
    mode = ?,
    status = ?,
    normalization_version = ?,
    total_rows = ?,
    auto_applied_rows = ?,
    review_rows = ?,
    failed_rows = ?,
    completed_at = ?
WHERE run_id = ?;
`, nullableStringValue(ptrToString(run.ImportID)), nullableStringValue(ptrToString(run.SourceReference)), run.Mode, run.Status, strings.TrimSpace(run.NormalizationVersion), run.TotalRows, run.AutoAppliedRows, run.ReviewRows, run.FailedRows, nullableStringValue(ptrToString(run.CompletedAt)), run.RunID)
	if err != nil {
		return fmt.Errorf("update location normalization run: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update location normalization run rows affected: %w", err)
	}
	if affected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Repository) createLocationNormalizationItem(ctx context.Context, item *model.LocationNormalizationItem) error {
	if item == nil {
		return fmt.Errorf("location normalization item is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO location_normalization_items (
    item_id,
    run_id,
    row_number,
    source_reference,
    raw_region,
    raw_province,
    raw_city,
    raw_barangay,
    resolved_region_code,
    resolved_region_name,
    resolved_province_code,
    resolved_province_name,
    resolved_city_code,
    resolved_city_name,
    resolved_barangay_code,
    resolved_barangay_name,
    confidence,
    match_source,
    status,
    needs_review,
    reason,
    normalization_version,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`, item.ItemID, item.RunID, item.RowNumber, nullableStringValue(ptrToString(item.SourceReference)), item.RawRegion, item.RawProvince, item.RawCity, item.RawBarangay, nullableStringValue(ptrToString(item.ResolvedRegionCode)), nullableStringValue(ptrToString(item.ResolvedRegionName)), nullableStringValue(ptrToString(item.ResolvedProvinceCode)), nullableStringValue(ptrToString(item.ResolvedProvinceName)), nullableStringValue(ptrToString(item.ResolvedCityCode)), nullableStringValue(ptrToString(item.ResolvedCityName)), nullableStringValue(ptrToString(item.ResolvedBarangayCode)), nullableStringValue(ptrToString(item.ResolvedBarangayName)), item.Confidence, item.MatchSource, item.Status, boolToInt(item.NeedsReview), nullableStringValue(ptrToString(item.Reason)), strings.TrimSpace(item.NormalizationVersion), item.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert location normalization item: %w", err)
	}

	return nil
}

func scanLocationNormalizationRun(scanner rowScanner) (*model.LocationNormalizationRun, error) {
	var (
		item            model.LocationNormalizationRun
		importID        sql.NullString
		sourceReference sql.NullString
		completedAt     sql.NullString
	)

	if err := scanner.Scan(&item.RunID, &importID, &sourceReference, &item.Mode, &item.Status, &item.NormalizationVersion, &item.TotalRows, &item.AutoAppliedRows, &item.ReviewRows, &item.FailedRows, &item.StartedAt, &completedAt); err != nil {
		return nil, err
	}

	item.ImportID = stringPtrFromNullString(importID)
	item.SourceReference = stringPtrFromNullString(sourceReference)
	item.CompletedAt = stringPtrFromNullString(completedAt)
	return &item, nil
}

func scanLocationNormalizationItem(scanner rowScanner) (*model.LocationNormalizationItem, error) {
	var (
		item                 model.LocationNormalizationItem
		sourceReference      sql.NullString
		resolvedRegionCode   sql.NullString
		resolvedRegionName   sql.NullString
		resolvedProvinceCode sql.NullString
		resolvedProvinceName sql.NullString
		resolvedCityCode     sql.NullString
		resolvedCityName     sql.NullString
		resolvedBarangayCode sql.NullString
		resolvedBarangayName sql.NullString
		needsReview          sql.NullInt64
		reason               sql.NullString
	)

	if err := scanner.Scan(&item.ItemID, &item.RunID, &item.RowNumber, &sourceReference, &item.RawRegion, &item.RawProvince, &item.RawCity, &item.RawBarangay, &resolvedRegionCode, &resolvedRegionName, &resolvedProvinceCode, &resolvedProvinceName, &resolvedCityCode, &resolvedCityName, &resolvedBarangayCode, &resolvedBarangayName, &item.Confidence, &item.MatchSource, &item.Status, &needsReview, &reason, &item.NormalizationVersion, &item.CreatedAt); err != nil {
		return nil, err
	}

	item.SourceReference = stringPtrFromNullString(sourceReference)
	item.ResolvedRegionCode = stringPtrFromNullString(resolvedRegionCode)
	item.ResolvedRegionName = stringPtrFromNullString(resolvedRegionName)
	item.ResolvedProvinceCode = stringPtrFromNullString(resolvedProvinceCode)
	item.ResolvedProvinceName = stringPtrFromNullString(resolvedProvinceName)
	item.ResolvedCityCode = stringPtrFromNullString(resolvedCityCode)
	item.ResolvedCityName = stringPtrFromNullString(resolvedCityName)
	item.ResolvedBarangayCode = stringPtrFromNullString(resolvedBarangayCode)
	item.ResolvedBarangayName = stringPtrFromNullString(resolvedBarangayName)
	item.NeedsReview = nullInt64(needsReview) == 1
	item.Reason = stringPtrFromNullString(reason)
	return &item, nil
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func ptrToString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}
