package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	defaultBeneficiaryLimit = 50
	defaultLookupLimit      = 25
	defaultGenericLimit     = 50
	maxGenericLimit         = 500
)

type rowScanner interface {
	Scan(dest ...any) error
}

type sqlBuilder struct {
	clauses []string
	args    []any
}

func background(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func normalizeLimit(limit, defaultLimit int) int {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxGenericLimit {
		limit = maxGenericLimit
	}
	return limit
}

func normalizeOffset(offset int) error {
	if offset < 0 {
		return fmt.Errorf("offset must be >= 0")
	}
	return nil
}

func (b *sqlBuilder) add(clause string, args ...any) {
	if clause == "" {
		return
	}
	b.clauses = append(b.clauses, clause)
	b.args = append(b.args, args...)
}

func (b *sqlBuilder) addEquals(column, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	b.add(column+" = ?", value)
}

func (b *sqlBuilder) addNotEquals(column, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	b.add(column+" <> ?", value)
}

func (b *sqlBuilder) addSearch(columns []string, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}

	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, column+" LIKE '%' || ? || '%' COLLATE NOCASE")
		b.args = append(b.args, value)
	}

	b.add("(" + strings.Join(parts, " OR ") + ")")
}

func (b *sqlBuilder) where() string {
	if len(b.clauses) == 0 {
		return "1=1"
	}
	return strings.Join(b.clauses, " AND ")
}

func nullString(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func stringPtrFromNullString(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	value := v.String
	return &value
}

func nullInt64(v sql.NullInt64) int64 {
	if !v.Valid {
		return 0
	}
	return v.Int64
}

func int64PtrFromNullInt64(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	value := v.Int64
	return &value
}

func nullFloat64(v sql.NullFloat64) float64 {
	if !v.Valid {
		return 0
	}
	return v.Float64
}

func stringValue(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func int64Value(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func float64Value(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableStringValue(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func nullableInt64Value(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableIntValue(v int) any {
	if v == 0 {
		return nil
	}
	return int64(v)
}

func boolPtrValue(v *bool) any {
	if v == nil {
		return nil
	}
	if *v {
		return 1
	}
	return 0
}

func boolPtrFromNullInt64(v sql.NullInt64) *bool {
	if !v.Valid {
		return nil
	}
	value := v.Int64 != 0
	return &value
}
