package model

// LocationNormalizationStatus captures the outcome class for one normalized row.
type LocationNormalizationStatus string

const (
	LocationNormalizationStatusAutoApplied  LocationNormalizationStatus = "AUTO_APPLIED"
	LocationNormalizationStatusReviewNeeded LocationNormalizationStatus = "REVIEW_NEEDED"
)

// LocationMatchSource identifies how the resolved chain was produced.
type LocationMatchSource string

const (
	LocationMatchSourceNone  LocationMatchSource = "NONE"
	LocationMatchSourceExact LocationMatchSource = "EXACT"
	LocationMatchSourceFuzzy LocationMatchSource = "FUZZY"
	LocationMatchSourceMixed LocationMatchSource = "MIXED"
)

// LocationNormalizationMode captures whether normalization runs in shadow or write mode.
type LocationNormalizationMode string

const (
	LocationNormalizationModeShadow LocationNormalizationMode = "SHADOW"
	LocationNormalizationModeWrite  LocationNormalizationMode = "WRITE"
)

// LocationChainRaw holds untrusted source location values.
type LocationChainRaw struct {
	Region   string
	Province string
	City     string
	Barangay string
}

// LocationChainResolved holds canonical PSGC chain values.
type LocationChainResolved struct {
	RegionCode   string
	RegionName   string
	ProvinceCode string
	ProvinceName string
	CityCode     string
	CityName     string
	BarangayCode string
	BarangayName string
}

// LocationNormalizationResult is the deterministic output of one chain normalization.
type LocationNormalizationResult struct {
	Raw                  LocationChainRaw
	Resolved             LocationChainResolved
	Confidence           float64
	MatchSource          LocationMatchSource
	Status               LocationNormalizationStatus
	NeedsReview          bool
	AutoApply            bool
	Reason               string
	NormalizationVersion string
}

// LocationNormalizationRun stores metadata for one normalization run.
type LocationNormalizationRun struct {
	RunID                string
	ImportID             *string
	SourceReference      *string
	Mode                 LocationNormalizationMode
	Status               string
	NormalizationVersion string
	TotalRows            int
	AutoAppliedRows      int
	ReviewRows           int
	FailedRows           int
	StartedAt            string
	CompletedAt          *string
}

// LocationNormalizationItem stores row-level normalization lineage.
type LocationNormalizationItem struct {
	ItemID               string
	RunID                string
	RowNumber            int
	SourceReference      *string
	RawRegion            string
	RawProvince          string
	RawCity              string
	RawBarangay          string
	ResolvedRegionCode   *string
	ResolvedRegionName   *string
	ResolvedProvinceCode *string
	ResolvedProvinceName *string
	ResolvedCityCode     *string
	ResolvedCityName     *string
	ResolvedBarangayCode *string
	ResolvedBarangayName *string
	Confidence           float64
	MatchSource          LocationMatchSource
	Status               LocationNormalizationStatus
	NeedsReview          bool
	Reason               *string
	NormalizationVersion string
	CreatedAt            string
}
