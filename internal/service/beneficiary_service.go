package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dedup/internal/db"
	"dedup/internal/model"
	"dedup/internal/repository"

	"github.com/google/uuid"
)

const (
	defaultGeneratedIDSequenceKey  = "beneficiary.generated_id"
	defaultGeneratedIDPrefix       = "G"
	defaultGeneratedIDWidth        = 6
	defaultDuplicateCandidateLimit = 25
	maxGeneratedIDAttempts         = 256
)

var whitespacePattern = regexp.MustCompile(`\s+`)

// BeneficiaryDraft captures untrusted beneficiary input from UI/import flows.
type BeneficiaryDraft struct {
	LastName      string
	FirstName     string
	MiddleName    string
	ExtensionName string

	RegionCode   string
	RegionName   string
	ProvinceCode string
	ProvinceName string
	CityCode     string
	CityName     string
	BarangayCode string
	BarangayName string

	ContactNo string

	BirthMonth   *int64
	BirthDay     *int64
	BirthYear    *int64
	BirthdateISO string

	Sex string
}

// CreateOptions controls create behavior, including import collision handling.
type CreateOptions struct {
	InternalUUID         string
	PreferredGeneratedID string
	SourceType           model.BeneficiarySource
	SourceReference      string
	RecordStatus         model.RecordStatus
	DedupStatus          model.DedupStatus
}

// DuplicatePrecheckPrompt is the contract used by UI/import flows before commit.
type DuplicatePrecheckPrompt struct {
	Lookup               repository.BeneficiaryDuplicateLookup
	Candidates           []model.Beneficiary
	ExactDuplicates      []model.Beneficiary
	HasExactDuplicate    bool
	RequiresConfirmation bool
	Message              string
}

// BeneficiaryService handles normalization, validation, create semantics, and duplicate prechecks.
type BeneficiaryService struct {
	db          *sql.DB
	writer      *db.WriterGuard
	repo        repository.BeneficiaryRepository
	now         func() time.Time
	sequenceKey string
	idPrefix    string
	idWidth     int
}

// Option configures BeneficiaryService behavior.
type Option func(*BeneficiaryService)

// WithClock overrides the clock for deterministic tests.
func WithClock(clock func() time.Time) Option {
	return func(s *BeneficiaryService) {
		if clock != nil {
			s.now = clock
		}
	}
}

// WithGeneratedIDFormat overrides the generated ID sequence key/prefix/width.
func WithGeneratedIDFormat(sequenceKey, prefix string, width int) Option {
	return func(s *BeneficiaryService) {
		if strings.TrimSpace(sequenceKey) != "" {
			s.sequenceKey = strings.TrimSpace(sequenceKey)
		}
		if strings.TrimSpace(prefix) != "" {
			s.idPrefix = strings.TrimSpace(prefix)
		}
		if width > 0 {
			s.idWidth = width
		}
	}
}

// NewBeneficiaryService builds a service on top of existing DB + writer + repository primitives.
func NewBeneficiaryService(database *sql.DB, writer *db.WriterGuard, repo repository.BeneficiaryRepository, opts ...Option) (*BeneficiaryService, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if writer == nil {
		return nil, fmt.Errorf("writer guard is nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("beneficiary repository is nil")
	}

	svc := &BeneficiaryService{
		db:          database,
		writer:      writer,
		repo:        repo,
		now:         func() time.Time { return time.Now().UTC() },
		sequenceKey: defaultGeneratedIDSequenceKey,
		idPrefix:    defaultGeneratedIDPrefix,
		idWidth:     defaultGeneratedIDWidth,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}

	return svc, nil
}

// NormalizeAndValidateDraft returns a deterministic normalized beneficiary payload.
func (s *BeneficiaryService) NormalizeAndValidateDraft(draft BeneficiaryDraft) (*model.Beneficiary, error) {
	return normalizeAndValidateDraft(draft)
}

// BuildDuplicatePrecheckPrompt resolves deterministic duplicate candidates and exact matches.
func (s *BeneficiaryService) BuildDuplicatePrecheckPrompt(ctx context.Context, draft BeneficiaryDraft, excludeInternalUUID string) (*DuplicatePrecheckPrompt, error) {
	if s == nil {
		return nil, fmt.Errorf("beneficiary service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	normalized, err := normalizeAndValidateDraft(draft)
	if err != nil {
		return nil, err
	}

	lookup := repository.BeneficiaryDuplicateLookup{
		ExcludeInternalUUID: strings.TrimSpace(excludeInternalUUID),
		NormLastName:        normalized.NormLastName,
		NormFirstName:       normalized.NormFirstName,
		NormMiddleName:      ptrToString(normalized.NormMiddleName),
		NormExtensionName:   ptrToString(normalized.NormExtensionName),
		BirthdateISO:        ptrToString(normalized.BirthdateISO),
		ContactNoNorm:       ptrToString(normalized.ContactNoNorm),
		RegionCode:          normalized.RegionCode,
		ProvinceCode:        normalized.ProvinceCode,
		CityCode:            normalized.CityCode,
		BarangayCode:        normalized.BarangayCode,
		Sex:                 normalized.Sex,
		IncludeDeleted:      false,
		Limit:               defaultDuplicateCandidateLimit,
	}

	candidates, err := s.repo.FindDuplicateBeneficiaries(ctx, lookup)
	if err != nil {
		return nil, fmt.Errorf("duplicate precheck lookup: %w", err)
	}

	exact := make([]model.Beneficiary, 0)
	for _, item := range candidates {
		if isExactDuplicate(item, normalized) {
			exact = append(exact, item)
		}
	}

	message := "No duplicate candidates found."
	switch {
	case len(exact) > 0:
		message = "Exact duplicate detected. Confirm before saving."
	case len(candidates) > 0:
		message = "Potential duplicate candidates found. Review suggested."
	}

	return &DuplicatePrecheckPrompt{
		Lookup:               lookup,
		Candidates:           candidates,
		ExactDuplicates:      exact,
		HasExactDuplicate:    len(exact) > 0,
		RequiresConfirmation: len(exact) > 0,
		Message:              message,
	}, nil
}

// CreateBeneficiary normalizes draft input, allocates or resolves immutable visible IDs, and persists.
func (s *BeneficiaryService) CreateBeneficiary(ctx context.Context, draft BeneficiaryDraft, opts CreateOptions) (*model.Beneficiary, error) {
	if s == nil {
		return nil, fmt.Errorf("beneficiary service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	normalized, err := normalizeAndValidateDraft(draft)
	if err != nil {
		return nil, err
	}

	nowUTC := s.now().UTC().Format(time.RFC3339Nano)
	normalized.InternalUUID = strings.TrimSpace(opts.InternalUUID)
	if normalized.InternalUUID == "" {
		normalized.InternalUUID = uuid.NewString()
	}
	normalized.CreatedAt = nowUTC
	normalized.UpdatedAt = nowUTC
	normalized.RecordStatus = normalizeRecordStatus(opts.RecordStatus)
	normalized.DedupStatus = normalizeDedupStatus(opts.DedupStatus)
	normalized.SourceType = normalizeSourceType(opts.SourceType)
	normalized.SourceReference = emptyStringToNil(strings.TrimSpace(opts.SourceReference))

	preferred := strings.TrimSpace(opts.PreferredGeneratedID)
	if preferred != "" {
		preferred = collapseWhitespace(preferred)
	}

	generatedID, sourceReference, err := s.resolveGeneratedID(ctx, preferred, normalized.SourceType, normalized.SourceReference)
	if err != nil {
		return nil, err
	}
	normalized.GeneratedID = generatedID
	normalized.SourceReference = sourceReference

	for attempt := 0; attempt < maxGeneratedIDAttempts; attempt++ {
		err = s.repo.CreateBeneficiary(ctx, normalized)
		if err == nil {
			return normalized, nil
		}
		if !isGeneratedIDUniqueConstraint(err) {
			return nil, err
		}

		nextID, nextErr := s.allocateUniqueGeneratedID(ctx)
		if nextErr != nil {
			return nil, nextErr
		}
		normalized.GeneratedID = nextID
	}

	return nil, fmt.Errorf("create beneficiary exhausted generated_id retries")
}

// SoftDeleteBeneficiary applies contract-safe soft delete semantics with UTC timestamp stamping.
func (s *BeneficiaryService) SoftDeleteBeneficiary(ctx context.Context, internalUUID string) error {
	if s == nil {
		return fmt.Errorf("beneficiary service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	internalUUID = strings.TrimSpace(internalUUID)
	if internalUUID == "" {
		return fmt.Errorf("internal uuid is required")
	}

	deletedAt := s.now().UTC().Format(time.RFC3339Nano)
	return s.repo.SoftDeleteBeneficiary(ctx, internalUUID, deletedAt)
}

func (s *BeneficiaryService) resolveGeneratedID(ctx context.Context, preferred string, sourceType model.BeneficiarySource, sourceReference *string) (string, *string, error) {
	if preferred == "" {
		id, err := s.allocateUniqueGeneratedID(ctx)
		return id, sourceReference, err
	}

	_, err := s.repo.GetBeneficiaryByGeneratedID(ctx, preferred)
	switch {
	case err == nil:
		id, allocErr := s.allocateUniqueGeneratedID(ctx)
		if allocErr != nil {
			return "", nil, allocErr
		}
		if sourceType == model.BeneficiarySourceImport {
			sourceReference = appendSourceGeneratedIDProvenance(sourceReference, preferred)
		}
		return id, sourceReference, nil
	case errors.Is(err, sql.ErrNoRows):
		return preferred, sourceReference, nil
	default:
		return "", nil, fmt.Errorf("check generated_id collision: %w", err)
	}
}

func (s *BeneficiaryService) allocateUniqueGeneratedID(ctx context.Context) (string, error) {
	for attempt := 0; attempt < maxGeneratedIDAttempts; attempt++ {
		value, err := s.nextSequenceValue(ctx)
		if err != nil {
			return "", err
		}
		candidate := s.formatGeneratedID(value)
		_, err = s.repo.GetBeneficiaryByGeneratedID(ctx, candidate)
		switch {
		case err == nil:
			continue
		case errors.Is(err, sql.ErrNoRows):
			return candidate, nil
		default:
			return "", fmt.Errorf("validate generated_id uniqueness: %w", err)
		}
	}

	return "", fmt.Errorf("unable to allocate unique generated_id after %d attempts", maxGeneratedIDAttempts)
}

func (s *BeneficiaryService) nextSequenceValue(ctx context.Context) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var value int64
	err := s.writer.WithWriteTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO id_sequences(sequence_key, last_value)
VALUES (?, 0);
`, s.sequenceKey); err != nil {
			return fmt.Errorf("initialize generated_id sequence: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `
UPDATE id_sequences
SET last_value = last_value + 1
WHERE sequence_key = ?;
`, s.sequenceKey); err != nil {
			return fmt.Errorf("increment generated_id sequence: %w", err)
		}

		if err := tx.QueryRowContext(ctx, `
SELECT last_value
FROM id_sequences
WHERE sequence_key = ?
LIMIT 1;
`, s.sequenceKey).Scan(&value); err != nil {
			return fmt.Errorf("read generated_id sequence: %w", err)
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (s *BeneficiaryService) formatGeneratedID(value int64) string {
	return fmt.Sprintf("%s-%0*d", s.idPrefix, s.idWidth, value)
}

func normalizeAndValidateDraft(draft BeneficiaryDraft) (*model.Beneficiary, error) {
	lastName, err := normalizeRequiredText("last_name", draft.LastName)
	if err != nil {
		return nil, err
	}
	firstName, err := normalizeRequiredText("first_name", draft.FirstName)
	if err != nil {
		return nil, err
	}
	regionCode, err := normalizeRequiredText("region_code", draft.RegionCode)
	if err != nil {
		return nil, err
	}
	regionName, err := normalizeRequiredText("region_name", draft.RegionName)
	if err != nil {
		return nil, err
	}
	provinceCode, err := normalizeRequiredText("province_code", draft.ProvinceCode)
	if err != nil {
		return nil, err
	}
	provinceName, err := normalizeRequiredText("province_name", draft.ProvinceName)
	if err != nil {
		return nil, err
	}
	cityCode, err := normalizeRequiredText("city_code", draft.CityCode)
	if err != nil {
		return nil, err
	}
	cityName, err := normalizeRequiredText("city_name", draft.CityName)
	if err != nil {
		return nil, err
	}
	barangayCode, err := normalizeRequiredText("barangay_code", draft.BarangayCode)
	if err != nil {
		return nil, err
	}
	barangayName, err := normalizeRequiredText("barangay_name", draft.BarangayName)
	if err != nil {
		return nil, err
	}
	sex, err := normalizeRequiredText("sex", draft.Sex)
	if err != nil {
		return nil, err
	}

	middleName := normalizeOptionalText(draft.MiddleName)
	extensionName := normalizeOptionalText(draft.ExtensionName)

	birthMonth, birthDay, birthYear, birthdateISO, err := normalizeBirthFields(draft.BirthMonth, draft.BirthDay, draft.BirthYear, draft.BirthdateISO)
	if err != nil {
		return nil, err
	}

	contactNo := normalizeOptionalText(draft.ContactNo)
	contactNoNorm := normalizeContact(contactNo)

	return &model.Beneficiary{
		LastName:          lastName,
		FirstName:         firstName,
		MiddleName:        emptyStringToNil(middleName),
		ExtensionName:     emptyStringToNil(extensionName),
		NormLastName:      strings.ToUpper(lastName),
		NormFirstName:     strings.ToUpper(firstName),
		NormMiddleName:    emptyStringToNil(strings.ToUpper(middleName)),
		NormExtensionName: emptyStringToNil(strings.ToUpper(extensionName)),
		RegionCode:        regionCode,
		RegionName:        regionName,
		ProvinceCode:      provinceCode,
		ProvinceName:      provinceName,
		CityCode:          cityCode,
		CityName:          cityName,
		BarangayCode:      barangayCode,
		BarangayName:      barangayName,
		ContactNo:         emptyStringToNil(contactNo),
		ContactNoNorm:     emptyStringToNil(contactNoNorm),
		BirthMonth:        birthMonth,
		BirthDay:          birthDay,
		BirthYear:         birthYear,
		BirthdateISO:      birthdateISO,
		Sex:               strings.ToUpper(sex),
	}, nil
}

func normalizeRequiredText(fieldName, value string) (string, error) {
	clean := collapseWhitespace(strings.TrimSpace(value))
	if clean == "" {
		return "", fmt.Errorf("%s is required", fieldName)
	}
	return clean, nil
}

func normalizeOptionalText(value string) string {
	return collapseWhitespace(strings.TrimSpace(value))
}

func collapseWhitespace(value string) string {
	if value == "" {
		return ""
	}
	return whitespacePattern.ReplaceAllString(value, " ")
}

func normalizeContact(contact string) string {
	if contact == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(contact))
	for _, r := range contact {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func normalizeBirthFields(month, day, year *int64, iso string) (*int64, *int64, *int64, *string, error) {
	iso = strings.TrimSpace(iso)
	if iso != "" {
		parsed, err := time.Parse("2006-01-02", iso)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("birthdate_iso must be YYYY-MM-DD")
		}

		pm := int64(parsed.Month())
		pd := int64(parsed.Day())
		py := int64(parsed.Year())
		if err := validateBirthParts(pm, pd, py); err != nil {
			return nil, nil, nil, nil, err
		}
		if month != nil && *month != pm {
			return nil, nil, nil, nil, fmt.Errorf("birth_month does not match birthdate_iso")
		}
		if day != nil && *day != pd {
			return nil, nil, nil, nil, fmt.Errorf("birth_day does not match birthdate_iso")
		}
		if year != nil && *year != py {
			return nil, nil, nil, nil, fmt.Errorf("birth_year does not match birthdate_iso")
		}

		formatted := parsed.Format("2006-01-02")
		return int64Ptr(pm), int64Ptr(pd), int64Ptr(py), &formatted, nil
	}

	if month == nil && day == nil && year == nil {
		return nil, nil, nil, nil, nil
	}
	if month == nil || day == nil || year == nil {
		return nil, nil, nil, nil, fmt.Errorf("birth_month, birth_day, and birth_year must all be provided together")
	}
	if err := validateBirthParts(*month, *day, *year); err != nil {
		return nil, nil, nil, nil, err
	}

	formatted := fmt.Sprintf("%04d-%02d-%02d", *year, *month, *day)
	return int64Ptr(*month), int64Ptr(*day), int64Ptr(*year), &formatted, nil
}

func validateBirthParts(month, day, year int64) error {
	if month < 1 || month > 12 {
		return fmt.Errorf("birth_month must be between 1 and 12")
	}
	if day < 1 || day > 31 {
		return fmt.Errorf("birth_day must be between 1 and 31")
	}
	if year < 1800 || year > 3000 {
		return fmt.Errorf("birth_year must be between 1800 and 3000")
	}

	parsed := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
	if int64(parsed.Month()) != month || int64(parsed.Day()) != day || int64(parsed.Year()) != year {
		return fmt.Errorf("birth date is not a valid calendar date")
	}

	return nil
}

func normalizeSourceType(value model.BeneficiarySource) model.BeneficiarySource {
	if strings.TrimSpace(string(value)) == "" {
		return model.BeneficiarySourceLocal
	}
	return model.BeneficiarySource(strings.ToUpper(strings.TrimSpace(string(value))))
}

func normalizeRecordStatus(value model.RecordStatus) model.RecordStatus {
	if strings.TrimSpace(string(value)) == "" {
		return model.RecordStatusActive
	}
	return model.RecordStatus(strings.ToUpper(strings.TrimSpace(string(value))))
}

func normalizeDedupStatus(value model.DedupStatus) model.DedupStatus {
	if strings.TrimSpace(string(value)) == "" {
		return model.DedupStatusClear
	}
	return model.DedupStatus(strings.ToUpper(strings.TrimSpace(string(value))))
}

func isExactDuplicate(candidate model.Beneficiary, normalized *model.Beneficiary) bool {
	if normalized == nil {
		return false
	}

	if candidate.NormLastName != normalized.NormLastName {
		return false
	}
	if candidate.NormFirstName != normalized.NormFirstName {
		return false
	}
	if !optionalStringMatches(candidate.NormMiddleName, normalized.NormMiddleName) {
		return false
	}
	if !optionalStringMatches(candidate.NormExtensionName, normalized.NormExtensionName) {
		return false
	}
	if candidate.RegionCode != normalized.RegionCode || candidate.ProvinceCode != normalized.ProvinceCode || candidate.CityCode != normalized.CityCode || candidate.BarangayCode != normalized.BarangayCode {
		return false
	}
	if !optionalStringMatches(candidate.BirthdateISO, normalized.BirthdateISO) {
		return false
	}
	if !optionalStringMatches(candidate.ContactNoNorm, normalized.ContactNoNorm) {
		return false
	}
	if candidate.Sex != normalized.Sex {
		return false
	}
	return true
}

func optionalStringMatches(left, right *string) bool {
	return normalizeNilString(left) == normalizeNilString(right)
}

func normalizeNilString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func appendSourceGeneratedIDProvenance(sourceReference *string, collidedGeneratedID string) *string {
	collidedGeneratedID = strings.TrimSpace(collidedGeneratedID)
	if collidedGeneratedID == "" {
		return sourceReference
	}

	marker := "source_generated_id=" + collidedGeneratedID
	existing := ""
	if sourceReference != nil {
		existing = strings.TrimSpace(*sourceReference)
	}
	if strings.Contains(existing, marker) {
		return emptyStringToNil(existing)
	}
	if existing == "" {
		return &marker
	}
	combined := existing + ";" + marker
	return &combined
}

func isGeneratedIDUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint failed") && strings.Contains(message, "beneficiaries.generated_id")
}

func ptrToString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func emptyStringToNil(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	v := value
	return &v
}

func int64Ptr(value int64) *int64 {
	v := value
	return &v
}

// mustAtoi is only used by tests through formatted IDs; kept private for predictable parsing.
func mustAtoi(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}
