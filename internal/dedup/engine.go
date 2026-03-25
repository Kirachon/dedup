package dedup

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"dedup/internal/model"
)

const (
	defaultThreshold = 90.0
)

const (
	weightFirstName     = 0.49
	weightMiddleName    = 0.05
	weightLastName      = 0.45
	weightExtensionName = 0.01
)

// CompareState is a typed compare state for optional field comparisons.
type CompareState int64

const (
	CompareStateUnknown   CompareState = 0
	CompareStateMatch     CompareState = 1
	CompareStateDifferent CompareState = 2
)

// Int64Ptr returns a pointer representation useful for persistence layers.
func (c CompareState) Int64Ptr() *int64 {
	v := int64(c)
	return &v
}

// NullableInt64Ptr returns nil for unknown and a pointer for known states.
func (c CompareState) NullableInt64Ptr() *int64 {
	if c == CompareStateUnknown {
		return nil
	}
	return c.Int64Ptr()
}

// RunRequest defines one deterministic dedup execution.
type RunRequest struct {
	RunID          string
	InitiatedBy    string
	Threshold      float64
	IncludeDeleted bool
}

// ScoredPair holds one scored candidate pair.
type ScoredPair struct {
	RecordAUUID string
	RecordBUUID string
	PairKey     string

	FirstNameScore     float64
	MiddleNameScore    float64
	LastNameScore      float64
	ExtensionNameScore float64
	TotalScore         float64

	BirthdateCompare CompareState
	BarangayCompare  CompareState
}

// RunResult is the deterministic output of one dedup run.
type RunResult struct {
	RunID           string
	Threshold       float64
	TotalCandidates int
	Matches         []ScoredPair
}

// Engine evaluates possible duplicate pairs from beneficiary data.
type Engine struct{}

// NewEngine creates a dedup engine.
func NewEngine() *Engine {
	return &Engine{}
}

// Run evaluates deterministic candidates and returns scored matches above threshold.
func (e *Engine) Run(request RunRequest, beneficiaries []model.Beneficiary) (RunResult, error) {
	threshold, err := normalizeThreshold(request.Threshold)
	if err != nil {
		return RunResult{}, err
	}

	filtered := filterBeneficiaries(beneficiaries, request.IncludeDeleted)
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].InternalUUID == filtered[j].InternalUUID {
			return filtered[i].GeneratedID < filtered[j].GeneratedID
		}
		return filtered[i].InternalUUID < filtered[j].InternalUUID
	})

	pairs := buildCandidatePairs(filtered)
	pairKeys := make([]string, 0, len(pairs))
	for pairKey := range pairs {
		pairKeys = append(pairKeys, pairKey)
	}
	sort.Strings(pairKeys)

	result := RunResult{
		RunID:           request.RunID,
		Threshold:       threshold,
		TotalCandidates: len(pairKeys),
		Matches:         make([]ScoredPair, 0),
	}

	for _, pairKey := range pairKeys {
		pair := pairs[pairKey]
		scored := scorePair(filtered[pair[0]], filtered[pair[1]])
		if scored.TotalScore >= threshold {
			result.Matches = append(result.Matches, scored)
		}
	}

	sort.Slice(result.Matches, func(i, j int) bool {
		if result.Matches[i].TotalScore == result.Matches[j].TotalScore {
			return result.Matches[i].PairKey < result.Matches[j].PairKey
		}
		return result.Matches[i].TotalScore > result.Matches[j].TotalScore
	})

	return result, nil
}

func normalizeThreshold(value float64) (float64, error) {
	if value == 0 {
		return defaultThreshold, nil
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("threshold must be a finite number")
	}
	if value < 0 || value > 100 {
		return 0, fmt.Errorf("threshold must be between 0 and 100")
	}
	return value, nil
}

func filterBeneficiaries(items []model.Beneficiary, includeDeleted bool) []model.Beneficiary {
	filtered := make([]model.Beneficiary, 0, len(items))
	for _, item := range items {
		if !includeDeleted && item.RecordStatus == model.RecordStatusDeleted {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func buildCandidatePairs(items []model.Beneficiary) map[string][2]int {
	blocks := make(map[string][]int)
	for i := range items {
		keys := blockingKeys(items[i])
		for _, key := range keys {
			blocks[key] = append(blocks[key], i)
		}
	}

	blockKeys := make([]string, 0, len(blocks))
	for key := range blocks {
		blockKeys = append(blockKeys, key)
	}
	sort.Strings(blockKeys)

	pairs := make(map[string][2]int)
	for _, blockKey := range blockKeys {
		indexes := blocks[blockKey]
		if len(indexes) < 2 {
			continue
		}
		for i := 0; i < len(indexes)-1; i++ {
			for j := i + 1; j < len(indexes); j++ {
				left := indexes[i]
				right := indexes[j]
				pairKey, a, b := canonicalPair(items[left], items[right], left, right)
				pairs[pairKey] = [2]int{a, b}
			}
		}
	}

	return pairs
}

func blockingKeys(item model.Beneficiary) []string {
	last := normalizedName(item.NormLastName, item.LastName)
	first := normalizedName(item.NormFirstName, item.FirstName)
	barangay := strings.TrimSpace(item.BarangayCode)
	year := birthYearString(item.BirthYear)

	set := map[string]struct{}{}
	add := func(key string) {
		if strings.TrimSpace(key) == "" {
			return
		}
		set[key] = struct{}{}
	}

	add("N|" + prefix(last, 4) + "|" + prefix(first, 4))
	add("L|" + prefix(last, 6))

	if barangay != "" {
		add("B|" + barangay + "|" + prefix(last, 3))
	}
	if year != "" {
		add("Y|" + year + "|" + prefix(last, 3))
	}

	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func canonicalPair(a, b model.Beneficiary, left, right int) (pairKey string, firstIndex int, secondIndex int) {
	if a.InternalUUID <= b.InternalUUID {
		return a.InternalUUID + "|" + b.InternalUUID, left, right
	}
	return b.InternalUUID + "|" + a.InternalUUID, right, left
}

func scorePair(a, b model.Beneficiary) ScoredPair {
	pairKey, _, _ := canonicalPair(a, b, 0, 1)
	firstNameScore := nameScore(normalizedName(a.NormFirstName, a.FirstName), normalizedName(b.NormFirstName, b.FirstName))
	middleNameScore := nameScore(normalizedOptionalName(a.NormMiddleName, a.MiddleName), normalizedOptionalName(b.NormMiddleName, b.MiddleName))
	lastNameScore := nameScore(normalizedName(a.NormLastName, a.LastName), normalizedName(b.NormLastName, b.LastName))
	extensionNameScore := nameScore(normalizedOptionalName(a.NormExtensionName, a.ExtensionName), normalizedOptionalName(b.NormExtensionName, b.ExtensionName))
	totalScore := weightedTotalScore(firstNameScore, middleNameScore, lastNameScore, extensionNameScore)

	recordAUUID := a.InternalUUID
	recordBUUID := b.InternalUUID
	if recordAUUID > recordBUUID {
		recordAUUID, recordBUUID = recordBUUID, recordAUUID
	}

	return ScoredPair{
		RecordAUUID:        recordAUUID,
		RecordBUUID:        recordBUUID,
		PairKey:            pairKey,
		FirstNameScore:     firstNameScore,
		MiddleNameScore:    middleNameScore,
		LastNameScore:      lastNameScore,
		ExtensionNameScore: extensionNameScore,
		TotalScore:         totalScore,
		BirthdateCompare:   compareBirthdate(a, b),
		BarangayCompare:    compareBarangay(a, b),
	}
}

func weightedTotalScore(first, middle, last, extension float64) float64 {
	return roundFloat((first*weightFirstName)+(middle*weightMiddleName)+(last*weightLastName)+(extension*weightExtensionName), 6)
}

func compareBirthdate(a, b model.Beneficiary) CompareState {
	left := canonicalBirthdate(a)
	right := canonicalBirthdate(b)
	if left == "" || right == "" {
		return CompareStateUnknown
	}
	if left == right {
		return CompareStateMatch
	}
	return CompareStateDifferent
}

func compareBarangay(a, b model.Beneficiary) CompareState {
	left := strings.TrimSpace(a.BarangayCode)
	right := strings.TrimSpace(b.BarangayCode)
	if left == "" || right == "" {
		return CompareStateUnknown
	}
	if left == right {
		return CompareStateMatch
	}
	return CompareStateDifferent
}

func canonicalBirthdate(item model.Beneficiary) string {
	if value := normalizedOptionalName(item.BirthdateISO, nil); value != "" {
		return value
	}
	if item.BirthYear == nil || item.BirthMonth == nil || item.BirthDay == nil {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02d", *item.BirthYear, *item.BirthMonth, *item.BirthDay)
}

func birthYearString(year *int64) string {
	if year == nil {
		return ""
	}
	return fmt.Sprintf("%04d", *year)
}

func normalizedName(primary, fallback string) string {
	value := collapseWhitespace(strings.ToUpper(strings.TrimSpace(primary)))
	if value != "" {
		return value
	}
	return collapseWhitespace(strings.ToUpper(strings.TrimSpace(fallback)))
}

func normalizedOptionalName(primary, fallback *string) string {
	if primary != nil {
		if v := collapseWhitespace(strings.ToUpper(strings.TrimSpace(*primary))); v != "" {
			return v
		}
	}
	if fallback != nil {
		return collapseWhitespace(strings.ToUpper(strings.TrimSpace(*fallback)))
	}
	return ""
}

func collapseWhitespace(value string) string {
	fields := strings.Fields(value)
	return strings.Join(fields, " ")
}

func prefix(value string, length int) string {
	runes := []rune(value)
	if len(runes) == 0 {
		return ""
	}
	if length <= 0 || len(runes) <= length {
		return string(runes)
	}
	return string(runes[:length])
}

func nameScore(left, right string) float64 {
	if left == "" && right == "" {
		return 100
	}
	if left == "" || right == "" {
		return 0
	}
	if left == right {
		return 100
	}

	dist := levenshteinDistance([]rune(left), []rune(right))
	maxLen := maxInt(len([]rune(left)), len([]rune(right)))
	if maxLen == 0 {
		return 100
	}
	score := 100 * (1 - (float64(dist) / float64(maxLen)))
	if score < 0 {
		score = 0
	}
	return roundFloat(score, 6)
}

func levenshteinDistance(left, right []rune) int {
	if len(left) == 0 {
		return len(right)
	}
	if len(right) == 0 {
		return len(left)
	}

	prev := make([]int, len(right)+1)
	curr := make([]int, len(right)+1)
	for j := 0; j <= len(right); j++ {
		prev[j] = j
	}

	for i := 1; i <= len(left); i++ {
		curr[0] = i
		for j := 1; j <= len(right); j++ {
			cost := 0
			if left[i-1] != right[j-1] {
				cost = 1
			}
			curr[j] = minInt(
				curr[j-1]+1,
				prev[j]+1,
				prev[j-1]+cost,
			)
		}
		copy(prev, curr)
	}

	return prev[len(right)]
}

func minInt(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] < min {
			min = values[i]
		}
	}
	return min
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func roundFloat(value float64, places int) float64 {
	if places <= 0 {
		return math.Round(value)
	}
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}
