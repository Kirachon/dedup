package repository

import (
	"context"
	"database/sql"
	"fmt"

	"dedup/internal/model"
)

// GetRegion loads one PSGC region by code.
func (r *Repository) GetRegion(ctx context.Context, regionCode string) (*model.PSGCRegion, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT region_code, region_name
FROM psgc_regions
WHERE region_code = ?
LIMIT 1;
`, regionCode)

	var item model.PSGCRegion
	if err := row.Scan(&item.RegionCode, &item.RegionName); err != nil {
		return nil, err
	}
	return &item, nil
}

// ListRegions returns all PSGC regions in name order.
func (r *Repository) ListRegions(ctx context.Context) ([]model.PSGCRegion, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT region_code, region_name
FROM psgc_regions
ORDER BY region_name ASC, region_code ASC;
`)
	if err != nil {
		return nil, fmt.Errorf("list regions: %w", err)
	}
	defer rows.Close()

	items := make([]model.PSGCRegion, 0)
	for rows.Next() {
		var item model.PSGCRegion
		if err := rows.Scan(&item.RegionCode, &item.RegionName); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate regions: %w", err)
	}

	return items, nil
}

// GetProvince loads one PSGC province by code.
func (r *Repository) GetProvince(ctx context.Context, provinceCode string) (*model.PSGCProvince, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT province_code, region_code, province_name
FROM psgc_provinces
WHERE province_code = ?
LIMIT 1;
`, provinceCode)

	var item model.PSGCProvince
	if err := row.Scan(&item.ProvinceCode, &item.RegionCode, &item.ProvinceName); err != nil {
		return nil, err
	}
	return &item, nil
}

// ListProvincesByRegion returns provinces for one region in name order.
func (r *Repository) ListProvincesByRegion(ctx context.Context, regionCode string) ([]model.PSGCProvince, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT province_code, region_code, province_name
FROM psgc_provinces
WHERE region_code = ?
ORDER BY province_name ASC, province_code ASC;
`, regionCode)
	if err != nil {
		return nil, fmt.Errorf("list provinces by region: %w", err)
	}
	defer rows.Close()

	items := make([]model.PSGCProvince, 0)
	for rows.Next() {
		var item model.PSGCProvince
		if err := rows.Scan(&item.ProvinceCode, &item.RegionCode, &item.ProvinceName); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provinces: %w", err)
	}

	return items, nil
}

// GetCity loads one PSGC city by code.
func (r *Repository) GetCity(ctx context.Context, cityCode string) (*model.PSGCCity, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT city_code, region_code, province_code, city_name, city_type
FROM psgc_cities
WHERE city_code = ?
LIMIT 1;
`, cityCode)

	var (
		item         model.PSGCCity
		provinceCode sql.NullString
		cityType     sql.NullString
	)
	if err := row.Scan(&item.CityCode, &item.RegionCode, &provinceCode, &item.CityName, &cityType); err != nil {
		return nil, err
	}
	item.ProvinceCode = stringPtrFromNullString(provinceCode)
	item.CityType = stringPtrFromNullString(cityType)
	return &item, nil
}

// ListCitiesByRegion returns cities for one region in name order.
func (r *Repository) ListCitiesByRegion(ctx context.Context, regionCode string) ([]model.PSGCCity, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT city_code, region_code, province_code, city_name, city_type
FROM psgc_cities
WHERE region_code = ?
ORDER BY city_name ASC, city_code ASC;
`, regionCode)
	if err != nil {
		return nil, fmt.Errorf("list cities by region: %w", err)
	}
	defer rows.Close()

	items := make([]model.PSGCCity, 0)
	for rows.Next() {
		var (
			item         model.PSGCCity
			provinceCode sql.NullString
			cityType     sql.NullString
		)
		if err := rows.Scan(&item.CityCode, &item.RegionCode, &provinceCode, &item.CityName, &cityType); err != nil {
			return nil, err
		}
		item.ProvinceCode = stringPtrFromNullString(provinceCode)
		item.CityType = stringPtrFromNullString(cityType)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cities by region: %w", err)
	}

	return items, nil
}

// ListCitiesByProvince returns cities for one province in name order.
func (r *Repository) ListCitiesByProvince(ctx context.Context, provinceCode string) ([]model.PSGCCity, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT city_code, region_code, province_code, city_name, city_type
FROM psgc_cities
WHERE province_code = ?
ORDER BY city_name ASC, city_code ASC;
`, provinceCode)
	if err != nil {
		return nil, fmt.Errorf("list cities by province: %w", err)
	}
	defer rows.Close()

	items := make([]model.PSGCCity, 0)
	for rows.Next() {
		var (
			item         model.PSGCCity
			provinceCode sql.NullString
			cityType     sql.NullString
		)
		if err := rows.Scan(&item.CityCode, &item.RegionCode, &provinceCode, &item.CityName, &cityType); err != nil {
			return nil, err
		}
		item.ProvinceCode = stringPtrFromNullString(provinceCode)
		item.CityType = stringPtrFromNullString(cityType)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cities by province: %w", err)
	}

	return items, nil
}

// GetBarangay loads one PSGC barangay by code.
func (r *Repository) GetBarangay(ctx context.Context, barangayCode string) (*model.PSGCBarangay, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT barangay_code, region_code, province_code, city_code, barangay_name
FROM psgc_barangays
WHERE barangay_code = ?
LIMIT 1;
`, barangayCode)

	var (
		item         model.PSGCBarangay
		provinceCode sql.NullString
	)
	if err := row.Scan(&item.BarangayCode, &item.RegionCode, &provinceCode, &item.CityCode, &item.BarangayName); err != nil {
		return nil, err
	}
	item.ProvinceCode = stringPtrFromNullString(provinceCode)
	return &item, nil
}

// ListBarangaysByCity returns barangays for one city in name order.
func (r *Repository) ListBarangaysByCity(ctx context.Context, cityCode string) ([]model.PSGCBarangay, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT barangay_code, region_code, province_code, city_code, barangay_name
FROM psgc_barangays
WHERE city_code = ?
ORDER BY barangay_name ASC, barangay_code ASC;
`, cityCode)
	if err != nil {
		return nil, fmt.Errorf("list barangays by city: %w", err)
	}
	defer rows.Close()

	items := make([]model.PSGCBarangay, 0)
	for rows.Next() {
		var (
			item         model.PSGCBarangay
			provinceCode sql.NullString
		)
		if err := rows.Scan(&item.BarangayCode, &item.RegionCode, &provinceCode, &item.CityCode, &item.BarangayName); err != nil {
			return nil, err
		}
		item.ProvinceCode = stringPtrFromNullString(provinceCode)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate barangays by city: %w", err)
	}

	return items, nil
}

// GetIngestMetadata loads the singleton PSGC ingest metadata row.
func (r *Repository) GetIngestMetadata(ctx context.Context) (*model.PSGCIngestMetadata, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT id, source_file_name, source_checksum, rows_read, rows_regions, rows_provinces, rows_cities, rows_barangays, ingested_at
FROM psgc_ingest_metadata
WHERE id = 1
LIMIT 1;
`)

	var item model.PSGCIngestMetadata
	if err := row.Scan(&item.ID, &item.SourceFileName, &item.SourceChecksum, &item.RowsRead, &item.RowsRegions, &item.RowsProvinces, &item.RowsCities, &item.RowsBarangays, &item.IngestedAt); err != nil {
		return nil, err
	}
	return &item, nil
}
