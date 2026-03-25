package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"dedup/internal/model"
)

// CreateBeneficiary inserts a new beneficiary row.
func (r *Repository) CreateBeneficiary(ctx context.Context, beneficiary *model.Beneficiary) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createBeneficiary(background(ctx), beneficiary)
	})
}

// UpdateBeneficiary updates an existing beneficiary row by internal UUID.
func (r *Repository) UpdateBeneficiary(ctx context.Context, beneficiary *model.Beneficiary) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.updateBeneficiary(background(ctx), beneficiary)
	})
}

// SoftDeleteBeneficiary marks the beneficiary as deleted without removing the row.
func (r *Repository) SoftDeleteBeneficiary(ctx context.Context, internalUUID, deletedAt string) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.softDeleteBeneficiary(background(ctx), internalUUID, deletedAt)
	})
}

// GetBeneficiary loads a beneficiary by internal UUID.
func (r *Repository) GetBeneficiary(ctx context.Context, internalUUID string) (*model.Beneficiary, error) {
	return r.getBeneficiaryByColumn(background(ctx), "internal_uuid", internalUUID)
}

// GetBeneficiaryByGeneratedID loads a beneficiary by generated ID.
func (r *Repository) GetBeneficiaryByGeneratedID(ctx context.Context, generatedID string) (*model.Beneficiary, error) {
	return r.getBeneficiaryByColumn(background(ctx), "generated_id", generatedID)
}

// ListBeneficiaries returns a deterministic page of beneficiaries.
func (r *Repository) ListBeneficiaries(ctx context.Context, query BeneficiaryListQuery) (BeneficiaryPage, error) {
	ctx = background(ctx)
	if err := normalizeOffset(query.Offset); err != nil {
		return BeneficiaryPage{}, err
	}

	limit := normalizeLimit(query.Limit, defaultBeneficiaryLimit)
	builder := sqlBuilder{}
	builder.addEquals("record_status", query.RecordStatus)
	builder.addEquals("dedup_status", query.DedupStatus)
	builder.addEquals("source_type", query.SourceType)
	builder.addEquals("region_code", query.RegionCode)
	builder.addEquals("province_code", query.ProvinceCode)
	builder.addEquals("city_code", query.CityCode)
	builder.addEquals("barangay_code", query.BarangayCode)
	if !query.IncludeDeleted {
		builder.add("record_status <> ?", "DELETED")
	}
	builder.addSearch([]string{
		"generated_id",
		"last_name",
		"first_name",
		"middle_name",
		"extension_name",
		"contact_no_norm",
		"source_reference",
	}, query.Search)

	q, err := r.queryer()
	if err != nil {
		return BeneficiaryPage{}, err
	}

	countSQL := "SELECT COUNT(*) FROM beneficiaries WHERE " + builder.where() + ";"
	var total int64
	if err := q.QueryRowContext(ctx, countSQL, builder.args...).Scan(&total); err != nil {
		return BeneficiaryPage{}, fmt.Errorf("count beneficiaries: %w", err)
	}

	selectSQL := `
SELECT
    internal_uuid,
    generated_id,
    last_name,
    first_name,
    middle_name,
    extension_name,
    norm_last_name,
    norm_first_name,
    norm_middle_name,
    norm_extension_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    barangay_code,
    barangay_name,
    contact_no,
    contact_no_norm,
    birth_month,
    birth_day,
    birth_year,
    birthdate_iso,
    sex,
    record_status,
    dedup_status,
    source_type,
    source_reference,
    created_at,
    updated_at,
    deleted_at
FROM beneficiaries
WHERE ` + builder.where() + `
ORDER BY last_name ASC, first_name ASC, middle_name ASC, extension_name ASC, generated_id ASC, internal_uuid ASC
LIMIT ? OFFSET ?;`

	rows, err := q.QueryContext(ctx, selectSQL, append(builder.args, limit, query.Offset)...)
	if err != nil {
		return BeneficiaryPage{}, fmt.Errorf("list beneficiaries: %w", err)
	}
	defer rows.Close()

	items := make([]model.Beneficiary, 0)
	for rows.Next() {
		item, err := scanBeneficiary(rows)
		if err != nil {
			return BeneficiaryPage{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return BeneficiaryPage{}, fmt.Errorf("iterate beneficiaries: %w", err)
	}

	return BeneficiaryPage{
		Items: items,
		Total: int(total),
	}, nil
}

// FindDuplicateBeneficiaries returns deterministic candidates for a normalized duplicate lookup.
func (r *Repository) FindDuplicateBeneficiaries(ctx context.Context, query BeneficiaryDuplicateLookup) ([]model.Beneficiary, error) {
	ctx = background(ctx)
	limit := normalizeLimit(query.Limit, defaultLookupLimit)
	if strings.TrimSpace(query.NormLastName) == "" || strings.TrimSpace(query.NormFirstName) == "" {
		return nil, fmt.Errorf("norm_last_name and norm_first_name are required")
	}

	builder := sqlBuilder{}
	builder.add("norm_last_name = ?", query.NormLastName)
	builder.add("norm_first_name = ?", query.NormFirstName)
	builder.addEquals("norm_middle_name", query.NormMiddleName)
	builder.addEquals("norm_extension_name", query.NormExtensionName)
	builder.addEquals("birthdate_iso", query.BirthdateISO)
	builder.addEquals("contact_no_norm", query.ContactNoNorm)
	builder.addEquals("region_code", query.RegionCode)
	builder.addEquals("province_code", query.ProvinceCode)
	builder.addEquals("city_code", query.CityCode)
	builder.addEquals("barangay_code", query.BarangayCode)
	builder.addEquals("sex", query.Sex)
	if strings.TrimSpace(query.ExcludeInternalUUID) != "" {
		builder.add("internal_uuid <> ?", query.ExcludeInternalUUID)
	}
	if !query.IncludeDeleted {
		builder.add("record_status <> ?", "DELETED")
	}

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	sqlText := `
SELECT
    internal_uuid,
    generated_id,
    last_name,
    first_name,
    middle_name,
    extension_name,
    norm_last_name,
    norm_first_name,
    norm_middle_name,
    norm_extension_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    barangay_code,
    barangay_name,
    contact_no,
    contact_no_norm,
    birth_month,
    birth_day,
    birth_year,
    birthdate_iso,
    sex,
    record_status,
    dedup_status,
    source_type,
    source_reference,
    created_at,
    updated_at,
    deleted_at
FROM beneficiaries
WHERE ` + builder.where() + `
ORDER BY norm_last_name ASC, norm_first_name ASC, norm_middle_name ASC, norm_extension_name ASC, generated_id ASC, internal_uuid ASC
LIMIT ?;`

	rows, err := q.QueryContext(ctx, sqlText, append(builder.args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("list duplicate candidates: %w", err)
	}
	defer rows.Close()

	items := make([]model.Beneficiary, 0)
	for rows.Next() {
		item, err := scanBeneficiary(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate duplicate candidates: %w", err)
	}

	return items, nil
}

func (r *Repository) createBeneficiary(ctx context.Context, beneficiary *model.Beneficiary) error {
	if beneficiary == nil {
		return fmt.Errorf("beneficiary is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	recordStatus := strings.TrimSpace(string(beneficiary.RecordStatus))
	if recordStatus == "" {
		recordStatus = "ACTIVE"
	}
	dedupStatus := strings.TrimSpace(string(beneficiary.DedupStatus))
	if dedupStatus == "" {
		dedupStatus = "CLEAR"
	}
	sourceType := strings.TrimSpace(string(beneficiary.SourceType))
	if sourceType == "" {
		sourceType = "LOCAL"
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO beneficiaries (
    internal_uuid,
    generated_id,
    last_name,
    first_name,
    middle_name,
    extension_name,
    norm_last_name,
    norm_first_name,
    norm_middle_name,
    norm_extension_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    barangay_code,
    barangay_name,
    contact_no,
    contact_no_norm,
    birth_month,
    birth_day,
    birth_year,
    birthdate_iso,
    sex,
    record_status,
    dedup_status,
    source_type,
    source_reference,
    created_at,
    updated_at,
    deleted_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`,
		beneficiary.InternalUUID,
		beneficiary.GeneratedID,
		beneficiary.LastName,
		beneficiary.FirstName,
		stringValue(beneficiary.MiddleName),
		stringValue(beneficiary.ExtensionName),
		beneficiary.NormLastName,
		beneficiary.NormFirstName,
		stringValue(beneficiary.NormMiddleName),
		stringValue(beneficiary.NormExtensionName),
		beneficiary.RegionCode,
		beneficiary.RegionName,
		beneficiary.ProvinceCode,
		beneficiary.ProvinceName,
		beneficiary.CityCode,
		beneficiary.CityName,
		beneficiary.BarangayCode,
		beneficiary.BarangayName,
		stringValue(beneficiary.ContactNo),
		stringValue(beneficiary.ContactNoNorm),
		int64Value(beneficiary.BirthMonth),
		int64Value(beneficiary.BirthDay),
		int64Value(beneficiary.BirthYear),
		stringValue(beneficiary.BirthdateISO),
		beneficiary.Sex,
		recordStatus,
		dedupStatus,
		sourceType,
		stringValue(beneficiary.SourceReference),
		beneficiary.CreatedAt,
		beneficiary.UpdatedAt,
		stringValue(beneficiary.DeletedAt),
	)
	if err != nil {
		return fmt.Errorf("insert beneficiary: %w", err)
	}

	return nil
}

func (r *Repository) updateBeneficiary(ctx context.Context, beneficiary *model.Beneficiary) error {
	if beneficiary == nil {
		return fmt.Errorf("beneficiary is nil")
	}

	current, err := r.GetBeneficiary(ctx, beneficiary.InternalUUID)
	if err != nil {
		return err
	}
	if current.GeneratedID != beneficiary.GeneratedID {
		return fmt.Errorf("generated_id is immutable")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE beneficiaries SET
    last_name = ?,
    first_name = ?,
    middle_name = ?,
    extension_name = ?,
    norm_last_name = ?,
    norm_first_name = ?,
    norm_middle_name = ?,
    norm_extension_name = ?,
    region_code = ?,
    region_name = ?,
    province_code = ?,
    province_name = ?,
    city_code = ?,
    city_name = ?,
    barangay_code = ?,
    barangay_name = ?,
    contact_no = ?,
    contact_no_norm = ?,
    birth_month = ?,
    birth_day = ?,
    birth_year = ?,
    birthdate_iso = ?,
    sex = ?,
    record_status = ?,
    dedup_status = ?,
    source_type = ?,
    source_reference = ?,
    updated_at = ?,
    deleted_at = ?
WHERE internal_uuid = ?;
`,
		beneficiary.LastName,
		beneficiary.FirstName,
		stringValue(beneficiary.MiddleName),
		stringValue(beneficiary.ExtensionName),
		beneficiary.NormLastName,
		beneficiary.NormFirstName,
		stringValue(beneficiary.NormMiddleName),
		stringValue(beneficiary.NormExtensionName),
		beneficiary.RegionCode,
		beneficiary.RegionName,
		beneficiary.ProvinceCode,
		beneficiary.ProvinceName,
		beneficiary.CityCode,
		beneficiary.CityName,
		beneficiary.BarangayCode,
		beneficiary.BarangayName,
		stringValue(beneficiary.ContactNo),
		stringValue(beneficiary.ContactNoNorm),
		int64Value(beneficiary.BirthMonth),
		int64Value(beneficiary.BirthDay),
		int64Value(beneficiary.BirthYear),
		stringValue(beneficiary.BirthdateISO),
		beneficiary.Sex,
		beneficiary.RecordStatus,
		beneficiary.DedupStatus,
		beneficiary.SourceType,
		stringValue(beneficiary.SourceReference),
		beneficiary.UpdatedAt,
		stringValue(beneficiary.DeletedAt),
		beneficiary.InternalUUID,
	)
	if err != nil {
		return fmt.Errorf("update beneficiary: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update beneficiary rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Repository) softDeleteBeneficiary(ctx context.Context, internalUUID, deletedAt string) error {
	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE beneficiaries SET
    record_status = ?,
    deleted_at = ?,
    updated_at = ?
WHERE internal_uuid = ?;
`, "DELETED", deletedAt, deletedAt, internalUUID)
	if err != nil {
		return fmt.Errorf("soft delete beneficiary: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("soft delete beneficiary rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Repository) getBeneficiaryByColumn(ctx context.Context, column, value string) (*model.Beneficiary, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(ctx, `
SELECT
    internal_uuid,
    generated_id,
    last_name,
    first_name,
    middle_name,
    extension_name,
    norm_last_name,
    norm_first_name,
    norm_middle_name,
    norm_extension_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    barangay_code,
    barangay_name,
    contact_no,
    contact_no_norm,
    birth_month,
    birth_day,
    birth_year,
    birthdate_iso,
    sex,
    record_status,
    dedup_status,
    source_type,
    source_reference,
    created_at,
    updated_at,
    deleted_at
FROM beneficiaries
WHERE `+column+` = ?
LIMIT 1;
`, value)

	item, err := scanBeneficiary(row)
	if err != nil {
		return nil, err
	}

	return &item, nil
}

func scanBeneficiary(scanner rowScanner) (model.Beneficiary, error) {
	var (
		item              model.Beneficiary
		middleName        sql.NullString
		extensionName     sql.NullString
		normMiddleName    sql.NullString
		normExtensionName sql.NullString
		contactNo         sql.NullString
		contactNoNorm     sql.NullString
		birthMonth        sql.NullInt64
		birthDay          sql.NullInt64
		birthYear         sql.NullInt64
		birthdateISO      sql.NullString
		sourceReference   sql.NullString
		deletedAt         sql.NullString
	)

	if err := scanner.Scan(
		&item.InternalUUID,
		&item.GeneratedID,
		&item.LastName,
		&item.FirstName,
		&middleName,
		&extensionName,
		&item.NormLastName,
		&item.NormFirstName,
		&normMiddleName,
		&normExtensionName,
		&item.RegionCode,
		&item.RegionName,
		&item.ProvinceCode,
		&item.ProvinceName,
		&item.CityCode,
		&item.CityName,
		&item.BarangayCode,
		&item.BarangayName,
		&contactNo,
		&contactNoNorm,
		&birthMonth,
		&birthDay,
		&birthYear,
		&birthdateISO,
		&item.Sex,
		&item.RecordStatus,
		&item.DedupStatus,
		&item.SourceType,
		&sourceReference,
		&item.CreatedAt,
		&item.UpdatedAt,
		&deletedAt,
	); err != nil {
		return model.Beneficiary{}, err
	}

	item.MiddleName = stringPtrFromNullString(middleName)
	item.ExtensionName = stringPtrFromNullString(extensionName)
	item.NormMiddleName = stringPtrFromNullString(normMiddleName)
	item.NormExtensionName = stringPtrFromNullString(normExtensionName)
	item.ContactNo = stringPtrFromNullString(contactNo)
	item.ContactNoNorm = stringPtrFromNullString(contactNoNorm)
	item.BirthMonth = int64PtrFromNullInt64(birthMonth)
	item.BirthDay = int64PtrFromNullInt64(birthDay)
	item.BirthYear = int64PtrFromNullInt64(birthYear)
	item.BirthdateISO = stringPtrFromNullString(birthdateISO)
	item.SourceReference = stringPtrFromNullString(sourceReference)
	item.DeletedAt = stringPtrFromNullString(deletedAt)

	return item, nil
}
