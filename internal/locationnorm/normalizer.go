package locationnorm

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"dedup/internal/model"
)

const (
	defaultConfidenceThreshold = 0.95
	defaultTieDelta            = 0.01
	defaultMaxCandidates       = 10

	// NormalizationVersion tags behavior so previews/checkpoints remain deterministic.
	NormalizationVersion = "v1-cleanlist-parity-region-aliases"
)

type chainRecord struct {
	regionCode   string
	regionName   string
	provinceCode string
	provinceName string
	cityCode     string
	cityName     string
	barangayCode string
	barangayName string
}

// Catalog stores PSGC lookup state used by the normalizer.
type Catalog struct {
	regions        []model.PSGCRegion
	provinces      []model.PSGCProvince
	cities         []model.PSGCCity
	barangays      []model.PSGCBarangay
	chains         []chainRecord
	regionByCode   map[string]model.PSGCRegion
	regionByAlias  map[string]string
	provinceByCode map[string]model.PSGCProvince
	cityByCode     map[string]model.PSGCCity
	barangayByCode map[string]model.PSGCBarangay
}

// NewCatalog builds a reusable in-memory catalog from PSGC tables.
func NewCatalog(
	regions []model.PSGCRegion,
	provinces []model.PSGCProvince,
	cities []model.PSGCCity,
	barangays []model.PSGCBarangay,
) (*Catalog, error) {
	if len(regions) == 0 || len(provinces) == 0 || len(cities) == 0 || len(barangays) == 0 {
		return nil, fmt.Errorf("psgc catalog requires non-empty region/province/city/barangay sets")
	}

	catalog := &Catalog{
		regions:        append([]model.PSGCRegion(nil), regions...),
		provinces:      append([]model.PSGCProvince(nil), provinces...),
		cities:         append([]model.PSGCCity(nil), cities...),
		barangays:      append([]model.PSGCBarangay(nil), barangays...),
		regionByCode:   make(map[string]model.PSGCRegion, len(regions)),
		regionByAlias:  make(map[string]string, len(regions)*4),
		provinceByCode: make(map[string]model.PSGCProvince, len(provinces)),
		cityByCode:     make(map[string]model.PSGCCity, len(cities)),
		barangayByCode: make(map[string]model.PSGCBarangay, len(barangays)),
		chains:         make([]chainRecord, 0, len(barangays)),
	}

	for _, item := range regions {
		code := strings.TrimSpace(item.RegionCode)
		if code == "" {
			continue
		}
		catalog.regionByCode[code] = item
		catalog.addRegionAliases(code, item.RegionName)
	}
	for _, item := range provinces {
		code := strings.TrimSpace(item.ProvinceCode)
		if code == "" {
			continue
		}
		catalog.provinceByCode[code] = item
	}
	for _, item := range cities {
		code := strings.TrimSpace(item.CityCode)
		if code == "" {
			continue
		}
		catalog.cityByCode[code] = item
	}
	for _, item := range barangays {
		code := strings.TrimSpace(item.BarangayCode)
		if code == "" {
			continue
		}
		catalog.barangayByCode[code] = item
	}

	for _, barangay := range barangays {
		city, ok := catalog.cityByCode[strings.TrimSpace(barangay.CityCode)]
		if !ok {
			continue
		}
		provinceCode := strings.TrimSpace(valueOrEmpty(barangay.ProvinceCode))
		if provinceCode == "" {
			provinceCode = strings.TrimSpace(valueOrEmpty(city.ProvinceCode))
		}
		province, ok := catalog.provinceByCode[provinceCode]
		if !ok {
			continue
		}
		region, ok := catalog.regionByCode[strings.TrimSpace(barangay.RegionCode)]
		if !ok {
			region, ok = catalog.regionByCode[strings.TrimSpace(province.RegionCode)]
		}
		if !ok {
			continue
		}

		catalog.chains = append(catalog.chains, chainRecord{
			regionCode:   strings.TrimSpace(region.RegionCode),
			regionName:   strings.TrimSpace(region.RegionName),
			provinceCode: strings.TrimSpace(province.ProvinceCode),
			provinceName: strings.TrimSpace(province.ProvinceName),
			cityCode:     strings.TrimSpace(city.CityCode),
			cityName:     strings.TrimSpace(city.CityName),
			barangayCode: strings.TrimSpace(barangay.BarangayCode),
			barangayName: strings.TrimSpace(barangay.BarangayName),
		})
	}

	if len(catalog.chains) == 0 {
		return nil, fmt.Errorf("psgc catalog does not contain usable hierarchy chains")
	}

	return catalog, nil
}

func (c *Catalog) addRegionAliases(regionCode, regionName string) {
	if c == nil {
		return
	}
	canonicalName := strings.TrimSpace(regionName)
	if canonicalName == "" {
		return
	}
	for _, alias := range regionAliasForms(regionCode, canonicalName) {
		key := normalizeLookup(alias)
		if key == "" {
			continue
		}
		if _, exists := c.regionByAlias[key]; exists {
			continue
		}
		c.regionByAlias[key] = canonicalName
	}
}

func (c *Catalog) canonicalizeRegionInput(value string) string {
	if c == nil {
		return sanitizeText(value)
	}
	key := normalizeLookup(value)
	if key == "" {
		return sanitizeText(value)
	}
	if canonical, ok := c.regionByAlias[key]; ok {
		return canonical
	}
	return sanitizeText(value)
}

// LocationNormalizer resolves raw location chains to canonical PSGC fields.
type LocationNormalizer struct {
	catalog             *Catalog
	confidenceThreshold float64
	tieDelta            float64
	maxCandidates       int
	version             string
}

// Option configures LocationNormalizer behavior.
type Option func(*LocationNormalizer)

// WithConfidenceThreshold overrides the non-exact auto-apply threshold.
func WithConfidenceThreshold(value float64) Option {
	return func(n *LocationNormalizer) {
		if value > 0 && value <= 1 {
			n.confidenceThreshold = value
		}
	}
}

// WithTieDelta overrides the tie suppression delta.
func WithTieDelta(value float64) Option {
	return func(n *LocationNormalizer) {
		if value > 0 && value <= 1 {
			n.tieDelta = value
		}
	}
}

// WithMaxCandidates overrides the maximum top candidates kept per level.
func WithMaxCandidates(value int) Option {
	return func(n *LocationNormalizer) {
		if value > 0 {
			n.maxCandidates = value
		}
	}
}

// New creates a location normalizer from an in-memory PSGC catalog.
func New(catalog *Catalog, opts ...Option) (*LocationNormalizer, error) {
	if catalog == nil {
		return nil, fmt.Errorf("catalog is nil")
	}

	normalizer := &LocationNormalizer{
		catalog:             catalog,
		confidenceThreshold: defaultConfidenceThreshold,
		tieDelta:            defaultTieDelta,
		maxCandidates:       defaultMaxCandidates,
		version:             NormalizationVersion,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(normalizer)
		}
	}

	return normalizer, nil
}

// NormalizeChain resolves one full location chain and enforces atomic apply decisions.
func (n *LocationNormalizer) NormalizeChain(raw model.LocationChainRaw) model.LocationNormalizationResult {
	cleanedRaw := sanitizeRawChain(raw)
	canonicalRaw := cleanedRaw
	if n != nil && n.catalog != nil {
		canonicalRaw.Region = n.catalog.canonicalizeRegionInput(cleanedRaw.Region)
	}

	result := model.LocationNormalizationResult{
		Raw:                  cleanedRaw,
		MatchSource:          model.LocationMatchSourceNone,
		Status:               model.LocationNormalizationStatusReviewNeeded,
		NeedsReview:          true,
		AutoApply:            false,
		NormalizationVersion: n.version,
	}

	if n == nil || n.catalog == nil {
		result.Reason = "normalizer catalog is not initialized"
		return result
	}

	working, confidence, source, reason := n.resolveChain(canonicalRaw)
	if reason != "" {
		result.Reason = reason
	}
	if source != model.LocationMatchSourceNone {
		result.MatchSource = source
	}
	result.Confidence = clamp01(confidence)
	result.Resolved = model.LocationChainResolved{
		RegionCode:   working.regionCode,
		RegionName:   working.regionName,
		ProvinceCode: working.provinceCode,
		ProvinceName: working.provinceName,
		CityCode:     working.cityCode,
		CityName:     working.cityName,
		BarangayCode: working.barangayCode,
		BarangayName: working.barangayName,
	}

	if reason == "" && n.chainReady(working) && n.validateChain(working) {
		result.Status = model.LocationNormalizationStatusAutoApplied
		result.NeedsReview = false
		result.AutoApply = true
		if result.MatchSource == model.LocationMatchSourceNone {
			result.MatchSource = model.LocationMatchSourceExact
		}
		if result.Confidence == 0 {
			result.Confidence = 1
		}
		result.Reason = ""
		return result
	}

	if result.Reason == "" {
		result.Reason = "could not resolve a unique full hierarchy chain"
	}
	return result
}

func (n *LocationNormalizer) resolveChain(raw model.LocationChainRaw) (chainRecord, float64, model.LocationMatchSource, string) {
	regionRaw := raw.Region
	provinceRaw := raw.Province
	cityRaw := raw.City
	barangayRaw := raw.Barangay

	candidates := make([]chainRecord, 0, len(n.catalog.chains))
	for _, entry := range n.catalog.chains {
		candidates = append(candidates, entry)
	}

	// Narrow using exact matches first (fast path).
	regionExact := filterCandidatesExact(candidates, regionRaw, func(c chainRecord) string { return c.regionCode }, func(c chainRecord) string { return c.regionName })
	if regionRaw != "" && len(regionExact) == 0 {
		regionExact = candidates
	}
	if regionRaw != "" && len(regionExact) > 0 && len(regionExact) < len(candidates) {
		candidates = regionExact
	}

	provinceExact := filterCandidatesExact(candidates, provinceRaw, func(c chainRecord) string { return c.provinceCode }, func(c chainRecord) string { return c.provinceName })
	if provinceRaw != "" && len(provinceExact) == 0 {
		provinceExact = candidates
	}
	if provinceRaw != "" && len(provinceExact) > 0 && len(provinceExact) < len(candidates) {
		candidates = provinceExact
	}

	cityExact := filterCandidatesExact(candidates, cityRaw, func(c chainRecord) string { return c.cityCode }, func(c chainRecord) string { return c.cityName })
	if cityRaw != "" && len(cityExact) == 0 {
		cityExact = candidates
	}
	if cityRaw != "" && len(cityExact) > 0 && len(cityExact) < len(candidates) {
		candidates = cityExact
	}

	barangayExact := filterCandidatesExact(candidates, barangayRaw, func(c chainRecord) string { return c.barangayCode }, func(c chainRecord) string { return c.barangayName })
	if barangayRaw != "" && len(barangayExact) == 0 {
		barangayExact = candidates
	}
	if barangayRaw != "" && len(barangayExact) > 0 && len(barangayExact) < len(candidates) {
		candidates = barangayExact
	}

	// If exact narrowing produced one chain, auto-accept exact.
	if len(candidates) == 1 && n.matchAllProvidedFieldsExact(candidates[0], raw) {
		return candidates[0], 1, model.LocationMatchSourceExact, ""
	}

	// Fuzzy fallback over remaining candidates using hierarchical chain scoring.
	scored := n.scoreCandidates(candidates, raw)
	if len(scored) == 0 {
		return chainRecord{}, 0, model.LocationMatchSourceNone, "no location candidate matched"
	}

	best := scored[0]
	if len(scored) > 1 && math.Abs(scored[0].score-scored[1].score) < n.tieDelta {
		return best.chain, best.score, model.LocationMatchSourceFuzzy, "ambiguous location candidates"
	}
	if best.score < n.confidenceThreshold {
		return best.chain, best.score, model.LocationMatchSourceFuzzy, "confidence below threshold"
	}

	source := model.LocationMatchSourceFuzzy
	if best.score == 1 {
		source = model.LocationMatchSourceExact
	}
	return best.chain, best.score, source, ""
}

type scoredChain struct {
	chain chainRecord
	score float64
}

func (n *LocationNormalizer) scoreCandidates(candidates []chainRecord, raw model.LocationChainRaw) []scoredChain {
	if len(candidates) == 0 {
		return nil
	}
	regionRaw := normalizeLocationKey(raw.Region)
	provinceRaw := normalizeLocationKey(raw.Province)
	cityRaw := normalizeLocationKey(raw.City)
	barangayRaw := normalizeLocationKey(raw.Barangay)
	if regionRaw == "" && provinceRaw == "" && cityRaw == "" && barangayRaw == "" {
		return nil
	}

	scored := make([]scoredChain, 0, len(candidates))
	for _, candidate := range candidates {
		sum := 0.0
		weight := 0.0

		if regionRaw != "" {
			score := jaroWinkler(regionRaw, normalizeLocationKey(candidate.regionName))
			sum += score
			weight += 1
		}
		if provinceRaw != "" {
			score := jaroWinkler(provinceRaw, normalizeLocationKey(candidate.provinceName))
			sum += score
			weight += 1
		}
		if cityRaw != "" {
			score := jaroWinkler(cityRaw, normalizeLocationKey(candidate.cityName))
			sum += score
			weight += 1
		}
		if barangayRaw != "" {
			score := jaroWinkler(barangayRaw, normalizeLocationKey(candidate.barangayName))
			sum += score
			weight += 1
		}
		if weight == 0 {
			continue
		}

		scored = append(scored, scoredChain{
			chain: candidate,
			score: sum / weight,
		})
	}

	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			left := scored[i].chain.regionCode + "|" + scored[i].chain.provinceCode + "|" + scored[i].chain.cityCode + "|" + scored[i].chain.barangayCode
			right := scored[j].chain.regionCode + "|" + scored[j].chain.provinceCode + "|" + scored[j].chain.cityCode + "|" + scored[j].chain.barangayCode
			return left < right
		}
		return scored[i].score > scored[j].score
	})

	if len(scored) > n.maxCandidates {
		scored = scored[:n.maxCandidates]
	}
	return scored
}

func (n *LocationNormalizer) chainReady(chain chainRecord) bool {
	return chain.regionCode != "" &&
		chain.provinceCode != "" &&
		chain.cityCode != "" &&
		chain.barangayCode != ""
}

func (n *LocationNormalizer) validateChain(chain chainRecord) bool {
	if !n.chainReady(chain) {
		return false
	}

	region, ok := n.catalog.regionByCode[chain.regionCode]
	if !ok {
		return false
	}
	province, ok := n.catalog.provinceByCode[chain.provinceCode]
	if !ok {
		return false
	}
	city, ok := n.catalog.cityByCode[chain.cityCode]
	if !ok {
		return false
	}
	barangay, ok := n.catalog.barangayByCode[chain.barangayCode]
	if !ok {
		return false
	}

	if strings.TrimSpace(province.RegionCode) != strings.TrimSpace(region.RegionCode) {
		return false
	}
	if strings.TrimSpace(city.RegionCode) != strings.TrimSpace(region.RegionCode) {
		return false
	}
	if city.ProvinceCode != nil && strings.TrimSpace(*city.ProvinceCode) != strings.TrimSpace(province.ProvinceCode) {
		return false
	}
	if strings.TrimSpace(barangay.RegionCode) != strings.TrimSpace(region.RegionCode) {
		return false
	}
	if barangay.ProvinceCode != nil && strings.TrimSpace(*barangay.ProvinceCode) != strings.TrimSpace(province.ProvinceCode) {
		return false
	}
	if strings.TrimSpace(barangay.CityCode) != strings.TrimSpace(city.CityCode) {
		return false
	}

	return true
}

func (n *LocationNormalizer) matchAllProvidedFieldsExact(chain chainRecord, raw model.LocationChainRaw) bool {
	if raw.Region != "" && !matchesExact(raw.Region, chain.regionCode, chain.regionName) {
		return false
	}
	if raw.Province != "" && !matchesExact(raw.Province, chain.provinceCode, chain.provinceName) {
		return false
	}
	if raw.City != "" && !matchesExact(raw.City, chain.cityCode, chain.cityName) {
		return false
	}
	if raw.Barangay != "" && !matchesExact(raw.Barangay, chain.barangayCode, chain.barangayName) {
		return false
	}
	return true
}

func filterCandidatesExact(
	candidates []chainRecord,
	raw string,
	codeFn func(chainRecord) string,
	nameFn func(chainRecord) string,
) []chainRecord {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return candidates
	}

	filtered := make([]chainRecord, 0, len(candidates))
	for _, item := range candidates {
		if matchesExact(raw, codeFn(item), nameFn(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func matchesExact(raw, code, name string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(code), raw) {
		return true
	}
	return normalizeLookup(raw) == normalizeLookup(name)
}

func sanitizeRawChain(raw model.LocationChainRaw) model.LocationChainRaw {
	return model.LocationChainRaw{
		Region:   sanitizeText(raw.Region),
		Province: sanitizeText(raw.Province),
		City:     sanitizeText(raw.City),
		Barangay: sanitizeText(raw.Barangay),
	}
}

func regionAliasForms(regionCode, regionName string) []string {
	aliases := []string{strings.TrimSpace(regionCode), strings.TrimSpace(regionName)}
	normalized := normalizeLookup(regionName)
	if normalized != "" {
		aliases = append(aliases, normalized)
	}

	tokens := strings.Fields(normalized)
	if len(tokens) > 1 && tokens[0] == "region" {
		aliasTokens := make([]string, 0, 2)
		for idx := 1; idx < len(tokens); idx++ {
			token := tokens[idx]
			if token == "region" {
				break
			}
			aliasTokens = append(aliasTokens, token)
			if len(aliasTokens) == 2 {
				break
			}
		}
		if len(aliasTokens) > 0 {
			joined := strings.Join(aliasTokens, " ")
			aliases = append(aliases, joined, strings.ReplaceAll(joined, " ", ""), "region "+joined)
		}
	}

	switch normalized {
	case "national capital region":
		aliases = append(aliases, "ncr")
	case "cordillera administrative region":
		aliases = append(aliases, "car")
	case "autonomous region in muslim mindanao":
		aliases = append(aliases, "armm")
	case "bangsamoro autonomous region in muslim mindanao":
		aliases = append(aliases, "barmm")
	}

	deduped := make([]string, 0, len(aliases))
	seen := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		key := normalizeLookup(alias)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, alias)
	}
	return deduped
}

func sanitizeText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func normalizeLookup(value string) string {
	return strings.ToLower(sanitizeText(value))
}

func normalizeLocationKey(value string) string {
	value = sanitizeText(value)
	if value == "" {
		return ""
	}

	var tokens []string
	var current strings.Builder
	for _, char := range value {
		switch {
		case char == 'ñ' || char == 'Ñ':
			current.WriteRune('n')
		case unicode.IsLetter(char) || unicode.IsDigit(char):
			for _, lower := range strings.ToLower(string(char)) {
				current.WriteRune(lower)
			}
		default:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return strings.Join(tokens, " ")
}

func jaroWinkler(left, right string) float64 {
	if left == "" || right == "" {
		return 0
	}
	if left == right {
		return 1
	}

	leftRunes := []rune(left)
	rightRunes := []rune(right)
	jaro := jaroSimilarity(leftRunes, rightRunes)
	prefix := commonPrefix(leftRunes, rightRunes, 4)
	score := jaro + (float64(prefix) * 0.1 * (1 - jaro))
	return clamp01(score)
}

func jaroSimilarity(left, right []rune) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}

	matchDistance := maxInt(len(left), len(right))/2 - 1
	if matchDistance < 0 {
		matchDistance = 0
	}

	leftMatches := make([]bool, len(left))
	rightMatches := make([]bool, len(right))

	matches := 0
	for i := range left {
		start := maxInt(0, i-matchDistance)
		end := minInt(i+matchDistance+1, len(right))
		for j := start; j < end; j++ {
			if rightMatches[j] || left[i] != right[j] {
				continue
			}
			leftMatches[i] = true
			rightMatches[j] = true
			matches++
			break
		}
	}
	if matches == 0 {
		return 0
	}

	transpositions := 0
	rightIndex := 0
	for i := range left {
		if !leftMatches[i] {
			continue
		}
		for rightIndex < len(right) && !rightMatches[rightIndex] {
			rightIndex++
		}
		if rightIndex < len(right) && left[i] != right[rightIndex] {
			transpositions++
		}
		rightIndex++
	}

	m := float64(matches)
	return ((m / float64(len(left))) + (m / float64(len(right))) + ((m - float64(transpositions)/2) / m)) / 3
}

func commonPrefix(left, right []rune, limit int) int {
	size := minInt(minInt(len(left), len(right)), limit)
	count := 0
	for i := 0; i < size; i++ {
		if left[i] != right[i] {
			break
		}
		count++
	}
	return count
}

func valueOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
