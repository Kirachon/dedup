package dedup

import (
	"reflect"
	"testing"
	"time"

	"dedup/internal/model"
)

func TestEngineRunDeterministicOrderingAndTieBreak(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	itemsA := []model.Beneficiary{
		beneficiaryFixture("uuid-c", "SMITH", "JOHN", "M", "", "0101", "1990-01-01", model.RecordStatusActive),
		beneficiaryFixture("uuid-a", "SMITH", "JOHN", "M", "", "0101", "1990-01-01", model.RecordStatusActive),
		beneficiaryFixture("uuid-b", "SMITH", "JOHN", "M", "", "0101", "1990-01-01", model.RecordStatusActive),
	}
	itemsB := []model.Beneficiary{
		itemsA[1],
		itemsA[2],
		itemsA[0],
	}

	resultA, err := engine.Run(RunRequest{RunID: "run-a", Threshold: 90}, itemsA)
	if err != nil {
		t.Fatalf("run A: %v", err)
	}
	resultB, err := engine.Run(RunRequest{RunID: "run-b", Threshold: 90}, itemsB)
	if err != nil {
		t.Fatalf("run B: %v", err)
	}

	if resultA.TotalCandidates != 3 {
		t.Fatalf("expected 3 total candidates, got %d", resultA.TotalCandidates)
	}
	if len(resultA.Matches) != 3 {
		t.Fatalf("expected 3 matches, got %d", len(resultA.Matches))
	}

	expectedOrder := []string{
		"uuid-a|uuid-b",
		"uuid-a|uuid-c",
		"uuid-b|uuid-c",
	}
	for i, pair := range resultA.Matches {
		if pair.PairKey != expectedOrder[i] {
			t.Fatalf("unexpected tie-break order at %d: want %s got %s", i, expectedOrder[i], pair.PairKey)
		}
	}

	if !reflect.DeepEqual(stripRunID(resultA), stripRunID(resultB)) {
		t.Fatalf("expected deterministic results across reordered input")
	}
}

func TestEngineRunThresholdAndDeletedFilter(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	activeA := beneficiaryFixture("uuid-a", "SMITH", "JOHN", "M", "", "0101", "1990-01-01", model.RecordStatusActive)
	activeB := beneficiaryFixture("uuid-b", "SMITH", "JON", "M", "", "0101", "1990-01-01", model.RecordStatusActive)
	deleted := beneficiaryFixture("uuid-c", "SMITH", "JOHN", "M", "", "0101", "1990-01-01", model.RecordStatusDeleted)

	highThreshold, err := engine.Run(RunRequest{RunID: "run-high", Threshold: 100}, []model.Beneficiary{activeA, activeB, deleted})
	if err != nil {
		t.Fatalf("run high threshold: %v", err)
	}
	if len(highThreshold.Matches) != 0 {
		t.Fatalf("expected no matches at threshold 100 for non-identical pair, got %d", len(highThreshold.Matches))
	}
	if highThreshold.TotalCandidates != 1 {
		t.Fatalf("expected deleted row excluded from candidates, got %d candidates", highThreshold.TotalCandidates)
	}

	includeDeleted, err := engine.Run(RunRequest{RunID: "run-include", Threshold: 100, IncludeDeleted: true}, []model.Beneficiary{activeA, deleted})
	if err != nil {
		t.Fatalf("run include deleted: %v", err)
	}
	if includeDeleted.TotalCandidates != 1 {
		t.Fatalf("expected one candidate including deleted row, got %d", includeDeleted.TotalCandidates)
	}
	if len(includeDeleted.Matches) != 1 {
		t.Fatalf("expected one exact match including deleted row, got %d", len(includeDeleted.Matches))
	}
}

func TestEngineCandidateBlocking(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	items := []model.Beneficiary{
		beneficiaryFixture("uuid-a", "ALPHA", "BEA", "M", "", "0101", "1990-01-01", model.RecordStatusActive),
		beneficiaryFixture("uuid-b", "ZULU", "MAX", "N", "", "0202", "1980-01-01", model.RecordStatusActive),
	}

	result, err := engine.Run(RunRequest{RunID: "run-block", Threshold: 90}, items)
	if err != nil {
		t.Fatalf("run blocking: %v", err)
	}
	if result.TotalCandidates != 0 {
		t.Fatalf("expected zero blocked candidates, got %d", result.TotalCandidates)
	}
	if len(result.Matches) != 0 {
		t.Fatalf("expected zero matches, got %d", len(result.Matches))
	}
}

func TestEngineBlockingAndMatchingIgnoreRawLocationLabels(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	canonical := []model.Beneficiary{
		beneficiaryFixture("uuid-a", "SMITH", "JOHN", "M", "", "010101001", "1990-01-01", model.RecordStatusActive),
		beneficiaryFixture("uuid-b", "SMITH", "JOHN", "M", "", "010101001", "1990-01-01", model.RecordStatusActive),
	}

	noisy := []model.Beneficiary{canonical[0], canonical[1]}
	noisy[0].RegionName = "REG!ON ???"
	noisy[0].ProvinceName = "PR0V!NCE ###"
	noisy[0].CityName = "C!TY @@@"
	noisy[0].BarangayName = "B4RANGAY ***"
	noisy[1].RegionName = "RANDOM REGION LABEL"
	noisy[1].ProvinceName = "RANDOM PROVINCE LABEL"
	noisy[1].CityName = "RANDOM CITY LABEL"
	noisy[1].BarangayName = "RANDOM BARANGAY LABEL"

	request := RunRequest{RunID: "run-location-label-noise", Threshold: 90}
	canonicalResult, err := engine.Run(request, canonical)
	if err != nil {
		t.Fatalf("run canonical data: %v", err)
	}
	noisyResult, err := engine.Run(request, noisy)
	if err != nil {
		t.Fatalf("run noisy labels data: %v", err)
	}

	if canonicalResult.TotalCandidates == 0 {
		t.Fatalf("expected canonical test data to generate candidates")
	}
	if len(canonicalResult.Matches) == 0 {
		t.Fatalf("expected canonical test data to generate at least one match")
	}
	if !reflect.DeepEqual(canonicalResult, noisyResult) {
		t.Fatalf("expected identical dedup result when only raw location labels change, canonical=%+v noisy=%+v", canonicalResult, noisyResult)
	}
}

func TestEngineCompareStates(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	left := beneficiaryFixture("uuid-a", "SMITH", "JOHN", "M", "", "0101", "1990-01-01", model.RecordStatusActive)
	rightMatch := beneficiaryFixture("uuid-b", "SMITH", "JOHN", "M", "", "0101", "1990-01-01", model.RecordStatusActive)
	rightDifferent := beneficiaryFixture("uuid-c", "SMITH", "JOHN", "M", "", "0202", "", model.RecordStatusActive)

	matchResult, err := engine.Run(RunRequest{RunID: "run-compare-match", Threshold: 90}, []model.Beneficiary{left, rightMatch})
	if err != nil {
		t.Fatalf("run compare match: %v", err)
	}
	if len(matchResult.Matches) != 1 {
		t.Fatalf("expected one match, got %d", len(matchResult.Matches))
	}
	if matchResult.Matches[0].BirthdateCompare != CompareStateMatch {
		t.Fatalf("expected birthdate compare match, got %d", matchResult.Matches[0].BirthdateCompare)
	}
	if matchResult.Matches[0].BarangayCompare != CompareStateMatch {
		t.Fatalf("expected barangay compare match, got %d", matchResult.Matches[0].BarangayCompare)
	}

	diffResult, err := engine.Run(RunRequest{RunID: "run-compare-diff", Threshold: 90}, []model.Beneficiary{left, rightDifferent})
	if err != nil {
		t.Fatalf("run compare diff: %v", err)
	}
	if len(diffResult.Matches) != 1 {
		t.Fatalf("expected one match, got %d", len(diffResult.Matches))
	}
	if diffResult.Matches[0].BirthdateCompare != CompareStateUnknown {
		t.Fatalf("expected birthdate compare unknown, got %d", diffResult.Matches[0].BirthdateCompare)
	}
	if diffResult.Matches[0].BarangayCompare != CompareStateDifferent {
		t.Fatalf("expected barangay compare different, got %d", diffResult.Matches[0].BarangayCompare)
	}
}

func TestWeightedTotalScore(t *testing.T) {
	t.Parallel()

	value := weightedTotalScore(80, 20, 100, 0)
	expected := 85.2
	if value != expected {
		t.Fatalf("unexpected weighted score: want %.6f got %.6f", expected, value)
	}
}

func TestEnginePlanMatchingAlgorithms(t *testing.T) {
	t.Parallel()

	jw := jaroWinklerScore("MARTHA", "MARHTA")
	lev := nameScore("MARTHA", "MARHTA")
	if jw <= lev {
		t.Fatalf("expected jaro-winkler to score higher than levenshtein for transposition pair: jw=%.6f lev=%.6f", jw, lev)
	}
	if jw <= 90 {
		t.Fatalf("expected jaro-winkler transposition pair to remain a strong match, got %.6f", jw)
	}

	if got := hammingScore("JR", "JR"); got != 100 {
		t.Fatalf("expected exact extension match to score 100, got %.6f", got)
	}
	if got := hammingScore("JR", "SR"); got != 50 {
		t.Fatalf("expected one-character hamming mismatch to score 50, got %.6f", got)
	}
	if got := hammingScore("JR", "JRS"); got != 0 {
		t.Fatalf("expected different length extension to score 0, got %.6f", got)
	}
}

func TestEngineBlockingKeysPlanStyle(t *testing.T) {
	t.Parallel()

	item := beneficiaryFixture("uuid-a", "SMITH", "JOHN", "M", "", "010101001", "1990-01-01", model.RecordStatusActive)
	keys := blockingKeys(item)
	expected := []string{
		"B|010101001|1990",
		"C|010101|J",
		"C|010101|S",
		"N|SMI|JOH",
	}
	if !reflect.DeepEqual(keys, expected) {
		t.Fatalf("unexpected blocking keys: want %#v got %#v", expected, keys)
	}
}

func beneficiaryFixture(uuid, last, first, middle, ext, barangay, birthdate string, status model.RecordStatus) model.Beneficiary {
	item := model.Beneficiary{
		InternalUUID:      uuid,
		GeneratedID:       "G-" + uuid,
		LastName:          last,
		FirstName:         first,
		NormLastName:      last,
		NormFirstName:     first,
		BarangayCode:      barangay,
		BirthdateISO:      nil,
		RecordStatus:      status,
		DedupStatus:       model.DedupStatusClear,
		SourceType:        model.BeneficiarySourceLocal,
		ProvinceCode:      "0101",
		RegionCode:        "01",
		CityCode:          "010101",
		ProvinceName:      "Province",
		RegionName:        "Region",
		CityName:          "City",
		BarangayName:      "Barangay",
		Sex:               "F",
		CreatedAt:         "2026-03-25T00:00:00Z",
		UpdatedAt:         "2026-03-25T00:00:00Z",
		NormMiddleName:    stringPtr(middle),
		NormExtensionName: stringPtr(ext),
		MiddleName:        stringPtr(middle),
		ExtensionName:     stringPtr(ext),
	}
	if middle == "" {
		item.NormMiddleName = nil
		item.MiddleName = nil
	}
	if ext == "" {
		item.NormExtensionName = nil
		item.ExtensionName = nil
	}
	if birthdate != "" {
		item.BirthdateISO = stringPtr(birthdate)
		if parsed, err := time.Parse("2006-01-02", birthdate); err == nil {
			month := int64(parsed.Month())
			day := int64(parsed.Day())
			year := int64(parsed.Year())
			item.BirthMonth = &month
			item.BirthDay = &day
			item.BirthYear = &year
		}
	}
	return item
}

func stripRunID(result RunResult) RunResult {
	result.RunID = ""
	return result
}

func stringPtr(value string) *string {
	return &value
}
