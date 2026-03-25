package model

// RecordStatus captures the lifecycle state of a beneficiary row.
type RecordStatus string

const (
	RecordStatusActive   RecordStatus = "ACTIVE"
	RecordStatusRetained RecordStatus = "RETAINED"
	RecordStatusDeleted  RecordStatus = "DELETED"
)

// DedupStatus captures the duplicate-resolution state of a beneficiary row.
type DedupStatus string

const (
	DedupStatusClear             DedupStatus = "CLEAR"
	DedupStatusPossibleDuplicate DedupStatus = "POSSIBLE_DUPLICATE"
	DedupStatusResolved          DedupStatus = "RESOLVED"
)

// BeneficiarySource identifies how a beneficiary row entered the system.
type BeneficiarySource string

const (
	BeneficiarySourceLocal  BeneficiarySource = "LOCAL"
	BeneficiarySourceImport BeneficiarySource = "IMPORT"
)

// ImportSource identifies the source format for an import log.
type ImportSource string

const (
	ImportSourceCSV             ImportSource = "CSV"
	ImportSourceExchangePackage ImportSource = "EXCHANGE_PACKAGE"
)

// DedupDecisionType captures the operator decision for a duplicate pair.
type DedupDecisionType string

const (
	DedupDecisionRetainA     DedupDecisionType = "RETAIN_A"
	DedupDecisionRetainB     DedupDecisionType = "RETAIN_B"
	DedupDecisionRetainBoth  DedupDecisionType = "RETAIN_BOTH"
	DedupDecisionDeleteASoft DedupDecisionType = "DELETE_A_SOFT"
	DedupDecisionDeleteBSoft DedupDecisionType = "DELETE_B_SOFT"
	DedupDecisionDifferent   DedupDecisionType = "DIFFERENT_PERSONS"
)

// Beneficiary represents a persisted beneficiary row.
type Beneficiary struct {
	InternalUUID string
	GeneratedID  string

	LastName          string
	FirstName         string
	MiddleName        *string
	ExtensionName     *string
	NormLastName      string
	NormFirstName     string
	NormMiddleName    *string
	NormExtensionName *string

	RegionCode   string
	RegionName   string
	ProvinceCode string
	ProvinceName string
	CityCode     string
	CityName     string
	BarangayCode string
	BarangayName string

	ContactNo     *string
	ContactNoNorm *string

	BirthMonth   *int64
	BirthDay     *int64
	BirthYear    *int64
	BirthdateISO *string
	Sex          string

	RecordStatus    RecordStatus
	DedupStatus     DedupStatus
	SourceType      BeneficiarySource
	SourceReference *string

	CreatedAt string
	UpdatedAt string
	DeletedAt *string
}

// BeneficiaryFilter constrains repository list queries.
type BeneficiaryFilter struct {
	Query          string
	RecordStatus   string
	DedupStatus    string
	RegionCode     string
	ProvinceCode   string
	CityCode       string
	BarangayCode   string
	IncludeDeleted bool
	Limit          int
	Offset         int
}

// BeneficiaryPage represents a paged beneficiary query result.
type BeneficiaryPage struct {
	Items []Beneficiary
	Total int
}

// AppSetting stores a key/value configuration row.
type AppSetting struct {
	SettingKey   string
	SettingValue string
	UpdatedAt    string
}

// AuditLog stores a user/action audit entry.
type AuditLog struct {
	AuditID     string
	EntityType  string
	EntityID    string
	Action      string
	PerformedBy string
	DetailsJSON *string
	CreatedAt   string
}

// DedupRun stores run metadata.
type DedupRun struct {
	RunID           string
	StartedAt       string
	CompletedAt     *string
	Status          string
	TotalCandidates int
	TotalMatches    int
	Notes           *string
}

// DedupMatch stores a scored candidate pair.
type DedupMatch struct {
	MatchID            string
	RunID              string
	RecordAUUID        string
	RecordBUUID        string
	PairKey            string
	FirstNameScore     float64
	MiddleNameScore    float64
	LastNameScore      float64
	ExtensionNameScore float64
	TotalScore         float64
	BirthdateCompare   *int64
	BarangayCompare    *int64
	DecisionStatus     string
	CreatedAt          string
}

// DedupDecision stores a persisted decision for a pair.
type DedupDecision struct {
	DecisionID  string
	PairKey     string
	RecordAUUID string
	RecordBUUID string
	Decision    DedupDecisionType
	ResolvedBy  string
	ResolvedAt  string
	Notes       *string
}

// ImportLog stores import run details.
type ImportLog struct {
	ImportID        string
	SourceType      ImportSource
	SourceReference string
	FileName        *string
	FileHash        *string
	IdempotencyKey  *string
	RowsRead        int
	RowsInserted    int
	RowsSkipped     int
	RowsFailed      int
	Status          string
	StartedAt       string
	CompletedAt     *string
	CheckpointToken *string
	OperatorName    *string
	Remarks         *string
}

// ExportLog stores export run details.
type ExportLog struct {
	ExportID     string
	FileName     string
	ExportType   string
	RowsExported int
	CreatedAt    string
	PerformedBy  *string
}

// PSGCRegion stores a region row.
type PSGCRegion struct {
	RegionCode string
	RegionName string
}

// PSGCProvince stores a province row.
type PSGCProvince struct {
	ProvinceCode string
	RegionCode   string
	ProvinceName string
}

// PSGCCity stores a city/municipality row.
type PSGCCity struct {
	CityCode     string
	RegionCode   string
	ProvinceCode *string
	CityName     string
	CityType     *string
}

// PSGCBarangay stores a barangay row.
type PSGCBarangay struct {
	BarangayCode          string
	RegionCode            string
	ProvinceCode          *string
	CityCode              string
	BarangayName          string
	UrbRur                string
	CongressionalDistrict string
}

// PSGCIngestMetadata stores the current source checksum and counts.
type PSGCIngestMetadata struct {
	ID             int
	SourceFileName string
	SourceChecksum string
	RowsRead       int
	RowsRegions    int
	RowsProvinces  int
	RowsCities     int
	RowsBarangays  int
	IngestedAt     string
}

// JobStateRecord stores the durable current state of a long-running job.
type JobStateRecord struct {
	JobID           string
	State           string
	UpdatedAt       string
	Attempt         int
	ProgressPercent *float64
	Message         string
	ErrorCode       *string
}
