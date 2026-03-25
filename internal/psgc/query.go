package psgc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Region is a selectable PSGC region.
type Region struct {
	Code string
	Name string
}

// Province is a selectable PSGC province.
type Province struct {
	Code       string
	RegionCode string
	Name       string
}

// City is a selectable PSGC city or municipality.
type City struct {
	Code         string
	RegionCode   string
	ProvinceCode string
	Name         string
	Type         string
}

// Barangay is a selectable PSGC barangay.
type Barangay struct {
	Code         string
	RegionCode   string
	ProvinceCode string
	CityCode     string
	Name         string
}

// BarangayPath captures the cascade lookup path for a single barangay.
type BarangayPath struct {
	Region   Region
	Province Province
	City     City
	Barangay Barangay
}

// ListRegions returns all regions ordered by code.
func ListRegions(ctx context.Context, database *sql.DB) ([]Region, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if database == nil {
		return nil, fmt.Errorf("db is nil")
	}

	rows, err := database.QueryContext(ctx, `
SELECT region_code, region_name
FROM psgc_regions
ORDER BY region_code;
`)
	if err != nil {
		return nil, fmt.Errorf("list regions: %w", err)
	}
	defer rows.Close()

	var regions []Region
	for rows.Next() {
		var region Region
		if err := rows.Scan(&region.Code, &region.Name); err != nil {
			return nil, fmt.Errorf("scan region: %w", err)
		}
		regions = append(regions, region)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate regions: %w", err)
	}

	return regions, nil
}

// ListProvincesByRegion returns provinces for a region ordered by code.
func ListProvincesByRegion(ctx context.Context, database *sql.DB, regionCode string) ([]Province, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if database == nil {
		return nil, fmt.Errorf("db is nil")
	}

	rows, err := database.QueryContext(ctx, `
SELECT province_code, region_code, province_name
FROM psgc_provinces
WHERE region_code = ?
ORDER BY province_code;
`, regionCode)
	if err != nil {
		return nil, fmt.Errorf("list provinces by region: %w", err)
	}
	defer rows.Close()

	var provinces []Province
	for rows.Next() {
		var province Province
		if err := rows.Scan(&province.Code, &province.RegionCode, &province.Name); err != nil {
			return nil, fmt.Errorf("scan province: %w", err)
		}
		provinces = append(provinces, province)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provinces: %w", err)
	}

	return provinces, nil
}

// ListCitiesByProvince returns cities for a province ordered by code.
func ListCitiesByProvince(ctx context.Context, database *sql.DB, provinceCode string) ([]City, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if database == nil {
		return nil, fmt.Errorf("db is nil")
	}

	rows, err := database.QueryContext(ctx, `
SELECT city_code, region_code, province_code, city_name, city_type
FROM psgc_cities
WHERE province_code = ?
ORDER BY city_code;
`, provinceCode)
	if err != nil {
		return nil, fmt.Errorf("list cities by province: %w", err)
	}
	defer rows.Close()

	var cities []City
	for rows.Next() {
		var city City
		var cityType sql.NullString
		if err := rows.Scan(&city.Code, &city.RegionCode, &city.ProvinceCode, &city.Name, &cityType); err != nil {
			return nil, fmt.Errorf("scan city: %w", err)
		}
		if cityType.Valid {
			city.Type = cityType.String
		}
		cities = append(cities, city)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cities: %w", err)
	}

	return cities, nil
}

// ListBarangaysByCity returns barangays for a city ordered by code.
func ListBarangaysByCity(ctx context.Context, database *sql.DB, cityCode string) ([]Barangay, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if database == nil {
		return nil, fmt.Errorf("db is nil")
	}

	rows, err := database.QueryContext(ctx, `
SELECT barangay_code, region_code, province_code, city_code, barangay_name
FROM psgc_barangays
WHERE city_code = ?
ORDER BY barangay_code;
`, cityCode)
	if err != nil {
		return nil, fmt.Errorf("list barangays by city: %w", err)
	}
	defer rows.Close()

	var barangays []Barangay
	for rows.Next() {
		var barangay Barangay
		if err := rows.Scan(&barangay.Code, &barangay.RegionCode, &barangay.ProvinceCode, &barangay.CityCode, &barangay.Name); err != nil {
			return nil, fmt.Errorf("scan barangay: %w", err)
		}
		barangays = append(barangays, barangay)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate barangays: %w", err)
	}

	return barangays, nil
}

// GetBarangayPath returns the full cascade path for a barangay code.
func GetBarangayPath(ctx context.Context, database *sql.DB, barangayCode string) (BarangayPath, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if database == nil {
		return BarangayPath{}, fmt.Errorf("db is nil")
	}
	if barangayCode == "" {
		return BarangayPath{}, fmt.Errorf("barangay code is required")
	}

	row := database.QueryRowContext(ctx, `
SELECT
    r.region_code,
    r.region_name,
    p.province_code,
    p.region_code,
    p.province_name,
    c.city_code,
    c.region_code,
    c.province_code,
    c.city_name,
    c.city_type,
    b.barangay_code,
    b.region_code,
    b.province_code,
    b.city_code,
    b.barangay_name
FROM psgc_barangays b
JOIN psgc_cities c ON c.city_code = b.city_code
JOIN psgc_provinces p ON p.province_code = b.province_code
JOIN psgc_regions r ON r.region_code = b.region_code
WHERE b.barangay_code = ?;
`, barangayCode)

	var path BarangayPath
	var cityType sql.NullString
	if err := row.Scan(
		&path.Region.Code,
		&path.Region.Name,
		&path.Province.Code,
		&path.Province.RegionCode,
		&path.Province.Name,
		&path.City.Code,
		&path.City.RegionCode,
		&path.City.ProvinceCode,
		&path.City.Name,
		&cityType,
		&path.Barangay.Code,
		&path.Barangay.RegionCode,
		&path.Barangay.ProvinceCode,
		&path.Barangay.CityCode,
		&path.Barangay.Name,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BarangayPath{}, fmt.Errorf("barangay %s not found", barangayCode)
		}
		return BarangayPath{}, fmt.Errorf("lookup barangay path: %w", err)
	}

	if cityType.Valid {
		path.City.Type = cityType.String
	}

	return path, nil
}
