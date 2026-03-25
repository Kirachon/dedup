package ui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

func init() {
	registerScreen(10, "Encoding", buildBeneficiaryScreen)
}

type beneficiaryFormValues struct {
	GeneratedID   string
	LastName      string
	FirstName     string
	MiddleName    string
	ExtensionName string
	RegionCode    string
	RegionName    string
	ProvinceCode  string
	ProvinceName  string
	CityCode      string
	CityName      string
	BarangayCode  string
	BarangayName  string
	ContactNo     string
	BirthMonth    string
	BirthDay      string
	BirthYear     string
	Sex           string
}

func buildBeneficiaryScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Encoding screen unavailable")
	}

	var (
		items        []model.Beneficiary
		selectedUUID string
	)

	type psgcSelectionState struct {
		RegionCode   string
		RegionName   string
		ProvinceCode string
		ProvinceName string
		CityCode     string
		CityName     string
		BarangayCode string
		BarangayName string
	}

	psgcState := psgcSelectionState{}
	var psgcUpdating bool

	regionSelect := widget.NewSelect(nil, nil)
	provinceSelect := widget.NewSelect(nil, nil)
	citySelect := widget.NewSelect(nil, nil)
	barangaySelect := widget.NewSelect(nil, nil)

	regionCodeValue := widget.NewLabel("Code: —")
	provinceCodeValue := widget.NewLabel("Code: —")
	cityCodeValue := widget.NewLabel("Code: —")
	barangayCodeValue := widget.NewLabel("Code: —")

	regionNameValue := widget.NewLabel("Name: —")
	provinceNameValue := widget.NewLabel("Name: —")
	cityNameValue := widget.NewLabel("Name: —")
	barangayNameValue := widget.NewLabel("Name: —")

	var regions []model.PSGCRegion
	var provinces []model.PSGCProvince
	var cities []model.PSGCCity
	var barangays []model.PSGCBarangay

	psgcDisplay := func(code, name string) string {
		code = strings.TrimSpace(code)
		name = strings.TrimSpace(name)
		switch {
		case code == "" && name == "":
			return ""
		case name == "":
			return code
		case code == "":
			return name
		default:
			return fmt.Sprintf("%s - %s", code, name)
		}
	}

	psgcCodeFromDisplay := func(value string) string {
		value = strings.TrimSpace(value)
		if value == "" {
			return ""
		}
		code, _, found := strings.Cut(value, " - ")
		if found {
			return strings.TrimSpace(code)
		}
		return value
	}

	psgcNameFromDisplay := func(value string) string {
		value = strings.TrimSpace(value)
		if value == "" {
			return ""
		}
		if _, name, found := strings.Cut(value, " - "); found {
			return strings.TrimSpace(name)
		}
		return value
	}

	regionOptionValues := func(items []model.PSGCRegion) []string {
		values := make([]string, 0, len(items))
		for _, item := range items {
			values = append(values, psgcDisplay(item.RegionCode, item.RegionName))
		}
		return values
	}

	loadRegions := func() error {
		items, err := runtime.Repository.ListRegions(context.Background())
		if err != nil {
			return err
		}
		regions = items
		regionSelect.SetOptions(regionOptionValues(regions))
		return nil
	}

	ensureRegionsLoaded := func() error {
		if len(regions) > 0 {
			return nil
		}
		return loadRegions()
	}

	provinceOptionValues := func(items []model.PSGCProvince) []string {
		values := make([]string, 0, len(items))
		for _, item := range items {
			values = append(values, psgcDisplay(item.ProvinceCode, item.ProvinceName))
		}
		return values
	}

	cityOptionValues := func(items []model.PSGCCity) []string {
		values := make([]string, 0, len(items))
		for _, item := range items {
			values = append(values, psgcDisplay(item.CityCode, item.CityName))
		}
		return values
	}

	barangayOptionValues := func(items []model.PSGCBarangay) []string {
		values := make([]string, 0, len(items))
		for _, item := range items {
			values = append(values, psgcDisplay(item.BarangayCode, item.BarangayName))
		}
		return values
	}

	findRegionByCode := func(code string) (model.PSGCRegion, bool) {
		code = strings.TrimSpace(code)
		for _, item := range regions {
			if item.RegionCode == code {
				return item, true
			}
		}
		return model.PSGCRegion{}, false
	}

	findRegionByName := func(name string) (model.PSGCRegion, bool) {
		name = strings.TrimSpace(name)
		for _, item := range regions {
			if strings.EqualFold(strings.TrimSpace(item.RegionName), name) {
				return item, true
			}
		}
		return model.PSGCRegion{}, false
	}

	findProvinceByCode := func(code string) (model.PSGCProvince, bool) {
		code = strings.TrimSpace(code)
		for _, item := range provinces {
			if item.ProvinceCode == code {
				return item, true
			}
		}
		return model.PSGCProvince{}, false
	}

	findProvinceByName := func(name string) (model.PSGCProvince, bool) {
		name = strings.TrimSpace(name)
		for _, item := range provinces {
			if strings.EqualFold(strings.TrimSpace(item.ProvinceName), name) {
				return item, true
			}
		}
		return model.PSGCProvince{}, false
	}

	findCityByCode := func(code string) (model.PSGCCity, bool) {
		code = strings.TrimSpace(code)
		for _, item := range cities {
			if item.CityCode == code {
				return item, true
			}
		}
		return model.PSGCCity{}, false
	}

	findCityByName := func(name string) (model.PSGCCity, bool) {
		name = strings.TrimSpace(name)
		for _, item := range cities {
			if strings.EqualFold(strings.TrimSpace(item.CityName), name) {
				return item, true
			}
		}
		return model.PSGCCity{}, false
	}

	findBarangayByCode := func(code string) (model.PSGCBarangay, bool) {
		code = strings.TrimSpace(code)
		for _, item := range barangays {
			if item.BarangayCode == code {
				return item, true
			}
		}
		return model.PSGCBarangay{}, false
	}

	findBarangayByName := func(name string) (model.PSGCBarangay, bool) {
		name = strings.TrimSpace(name)
		for _, item := range barangays {
			if strings.EqualFold(strings.TrimSpace(item.BarangayName), name) {
				return item, true
			}
		}
		return model.PSGCBarangay{}, false
	}

	clearProvinceAndBelowSelections := func() {
		provinces = nil
		cities = nil
		barangays = nil
		provinceSelect.SetOptions(nil)
		citySelect.SetOptions(nil)
		barangaySelect.SetOptions(nil)
		provinceSelect.SetSelected("")
		citySelect.SetSelected("")
		barangaySelect.SetSelected("")
		provinceCodeValue.SetText("Code: —")
		provinceNameValue.SetText("Name: —")
		cityCodeValue.SetText("Code: —")
		cityNameValue.SetText("Name: —")
		barangayCodeValue.SetText("Code: —")
		barangayNameValue.SetText("Name: —")
		psgcState.ProvinceCode = ""
		psgcState.ProvinceName = ""
		psgcState.CityCode = ""
		psgcState.CityName = ""
		psgcState.BarangayCode = ""
		psgcState.BarangayName = ""
	}

	resetProvinceSelection := func() {
		psgcState.ProvinceCode = ""
		psgcState.ProvinceName = ""
		provinceSelect.SetSelected("")
		provinceCodeValue.SetText("Code: —")
		provinceNameValue.SetText("Name: —")
		cities = nil
		citySelect.SetOptions(nil)
		citySelect.SetSelected("")
		cityCodeValue.SetText("Code: —")
		cityNameValue.SetText("Name: —")
		barangays = nil
		barangaySelect.SetOptions(nil)
		barangaySelect.SetSelected("")
		barangayCodeValue.SetText("Code: —")
		barangayNameValue.SetText("Name: —")
		psgcState.CityCode = ""
		psgcState.CityName = ""
		psgcState.BarangayCode = ""
		psgcState.BarangayName = ""
	}

	resetCitySelection := func() {
		psgcState.CityCode = ""
		psgcState.CityName = ""
		citySelect.SetSelected("")
		cityCodeValue.SetText("Code: —")
		cityNameValue.SetText("Name: —")
		barangays = nil
		barangaySelect.SetOptions(nil)
		barangaySelect.SetSelected("")
		barangayCodeValue.SetText("Code: —")
		barangayNameValue.SetText("Name: —")
		psgcState.BarangayCode = ""
		psgcState.BarangayName = ""
	}

	resetBarangaySelection := func() {
		psgcState.BarangayCode = ""
		psgcState.BarangayName = ""
		barangaySelect.SetSelected("")
		barangayCodeValue.SetText("Code: —")
		barangayNameValue.SetText("Name: —")
	}

	loadProvinces := func(regionCode string) error {
		items, err := runtime.Repository.ListProvincesByRegion(context.Background(), regionCode)
		if err != nil {
			return err
		}
		provinces = items
		provinceSelect.SetOptions(provinceOptionValues(provinces))
		return nil
	}

	loadCities := func(provinceCode string) error {
		items, err := runtime.Repository.ListCitiesByProvince(context.Background(), provinceCode)
		if err != nil {
			return err
		}
		cities = items
		citySelect.SetOptions(cityOptionValues(cities))
		return nil
	}

	loadBarangays := func(cityCode string) error {
		items, err := runtime.Repository.ListBarangaysByCity(context.Background(), cityCode)
		if err != nil {
			return err
		}
		barangays = items
		barangaySelect.SetOptions(barangayOptionValues(barangays))
		return nil
	}

	applyPSGCSelection := func(regionCode, regionName, provinceCode, provinceName, cityCode, cityName, barangayCode, barangayName string) error {
		if psgcUpdating {
			return nil
		}
		psgcUpdating = true
		defer func() {
			psgcUpdating = false
		}()

		regionCode = strings.TrimSpace(regionCode)
		regionName = strings.TrimSpace(regionName)
		provinceCode = strings.TrimSpace(provinceCode)
		provinceName = strings.TrimSpace(provinceName)
		cityCode = strings.TrimSpace(cityCode)
		cityName = strings.TrimSpace(cityName)
		barangayCode = strings.TrimSpace(barangayCode)
		barangayName = strings.TrimSpace(barangayName)

		if regionCode == "" {
			if regionName == "" {
				regionSelect.SetSelected("")
				regionCodeValue.SetText("Code: —")
				regionNameValue.SetText("Name: —")
				clearProvinceAndBelowSelections()
				psgcState.RegionCode = ""
				psgcState.RegionName = ""
				return nil
			}
		}

		if err := ensureRegionsLoaded(); err != nil {
			return err
		}

		regionItem, ok := findRegionByCode(regionCode)
		if !ok && regionName != "" {
			regionItem, ok = findRegionByName(regionName)
		}
		if !ok {
			regionSelect.SetSelected("")
			regionCodeValue.SetText("Code: —")
			regionNameValue.SetText("Name: —")
			clearProvinceAndBelowSelections()
			psgcState.RegionCode = ""
			psgcState.RegionName = ""
			return nil
		}

		psgcState.RegionCode = regionItem.RegionCode
		psgcState.RegionName = regionItem.RegionName
		regionSelect.SetSelected(psgcDisplay(regionItem.RegionCode, regionItem.RegionName))
		regionCodeValue.SetText("Code: " + regionItem.RegionCode)
		regionNameValue.SetText(regionItem.RegionName)

		if err := loadProvinces(regionItem.RegionCode); err != nil {
			return err
		}
		if len(provinces) == 0 {
			clearProvinceAndBelowSelections()
			return nil
		}

		if provinceCode == "" && provinceName == "" {
			resetProvinceSelection()
			return nil
		}

		provinceItem, provinceFound := findProvinceByCode(provinceCode)
		if !provinceFound && provinceName != "" {
			provinceItem, provinceFound = findProvinceByName(provinceName)
		}
		if !provinceFound {
			resetProvinceSelection()
			return nil
		}

		psgcState.ProvinceCode = provinceItem.ProvinceCode
		psgcState.ProvinceName = provinceItem.ProvinceName
		provinceSelect.SetSelected(psgcDisplay(provinceItem.ProvinceCode, provinceItem.ProvinceName))
		provinceCodeValue.SetText("Code: " + provinceItem.ProvinceCode)
		provinceNameValue.SetText(provinceItem.ProvinceName)

		if err := loadCities(provinceItem.ProvinceCode); err != nil {
			return err
		}
		if len(cities) == 0 {
			resetCitySelection()
			return nil
		}

		if cityCode == "" && cityName == "" {
			resetCitySelection()
			return nil
		}

		cityItem, cityFound := findCityByCode(cityCode)
		if !cityFound && cityName != "" {
			cityItem, cityFound = findCityByName(cityName)
		}
		if !cityFound {
			resetCitySelection()
			return nil
		}

		psgcState.CityCode = cityItem.CityCode
		psgcState.CityName = cityItem.CityName
		citySelect.SetSelected(psgcDisplay(cityItem.CityCode, cityItem.CityName))
		cityCodeValue.SetText("Code: " + cityItem.CityCode)
		cityNameValue.SetText(cityItem.CityName)

		if err := loadBarangays(cityItem.CityCode); err != nil {
			return err
		}
		if len(barangays) == 0 {
			resetBarangaySelection()
			return nil
		}

		if barangayCode == "" && barangayName == "" {
			resetBarangaySelection()
			return nil
		}

		barangayItem, barangayFound := findBarangayByCode(barangayCode)
		if !barangayFound && barangayName != "" {
			barangayItem, barangayFound = findBarangayByName(barangayName)
		}
		if !barangayFound {
			resetBarangaySelection()
			return nil
		}

		psgcState.BarangayCode = barangayItem.BarangayCode
		psgcState.BarangayName = barangayItem.BarangayName
		barangaySelect.SetSelected(psgcDisplay(barangayItem.BarangayCode, barangayItem.BarangayName))
		barangayCodeValue.SetText("Code: " + barangayItem.BarangayCode)
		barangayNameValue.SetText(barangayItem.BarangayName)
		return nil
	}

	regionSelect.OnChanged = func(value string) {
		if psgcUpdating {
			return
		}
		if err := applyPSGCSelection(psgcCodeFromDisplay(value), psgcNameFromDisplay(value), "", "", "", "", "", ""); err != nil {
			runtime.SetStatus("PSGC region load failed")
			runtime.SetActivity(err.Error())
		}
	}

	provinceSelect.OnChanged = func(value string) {
		if psgcUpdating {
			return
		}
		if err := applyPSGCSelection(psgcState.RegionCode, psgcState.RegionName, psgcCodeFromDisplay(value), psgcNameFromDisplay(value), "", "", "", ""); err != nil {
			runtime.SetStatus("PSGC province load failed")
			runtime.SetActivity(err.Error())
		}
	}

	citySelect.OnChanged = func(value string) {
		if psgcUpdating {
			return
		}
		if err := applyPSGCSelection(psgcState.RegionCode, psgcState.RegionName, psgcState.ProvinceCode, psgcState.ProvinceName, psgcCodeFromDisplay(value), psgcNameFromDisplay(value), "", ""); err != nil {
			runtime.SetStatus("PSGC city load failed")
			runtime.SetActivity(err.Error())
		}
	}

	barangaySelect.OnChanged = func(value string) {
		if psgcUpdating {
			return
		}
		code := psgcCodeFromDisplay(value)
		name := psgcNameFromDisplay(value)
		if code == "" {
			if name == "" {
				psgcState.BarangayCode = ""
				psgcState.BarangayName = ""
				barangayCodeValue.SetText("Code: —")
				barangayNameValue.SetText("Name: —")
				return
			}
		}
		if code != "" {
			if item, ok := findBarangayByCode(code); ok {
				psgcState.BarangayCode = item.BarangayCode
				psgcState.BarangayName = item.BarangayName
				barangayCodeValue.SetText("Code: " + item.BarangayCode)
				barangayNameValue.SetText(item.BarangayName)
				return
			}
		}
		if name != "" {
			if item, ok := findBarangayByName(name); ok {
				psgcState.BarangayCode = item.BarangayCode
				psgcState.BarangayName = item.BarangayName
				barangaySelect.SetSelected(psgcDisplay(item.BarangayCode, item.BarangayName))
				barangayCodeValue.SetText("Code: " + item.BarangayCode)
				barangayNameValue.SetText(item.BarangayName)
			}
			return
		}
	}

	searchEntry := widget.NewEntry()
	searchEntry.SetPlaceHolder("Search by ID, name, contact, or source reference")
	resultLabel := widget.NewLabel("Results: 0")
	duplicateLabel := widget.NewLabel("Precheck ready")

	generatedIDEntry := widget.NewEntry()
	generatedIDEntry.SetPlaceHolder("Leave blank to auto-generate an ID")
	lastNameEntry := widget.NewEntry()
	firstNameEntry := widget.NewEntry()
	middleNameEntry := widget.NewEntry()
	extensionNameEntry := widget.NewEntry()
	contactNoEntry := widget.NewEntry()
	birthMonthEntry := widget.NewEntry()
	birthMonthEntry.SetPlaceHolder("MM")
	birthDayEntry := widget.NewEntry()
	birthDayEntry.SetPlaceHolder("DD")
	birthYearEntry := widget.NewEntry()
	birthYearEntry.SetPlaceHolder("YYYY")
	sexEntry := widget.NewEntry()
	sexEntry.SetPlaceHolder("M or F")

	clearForm := func() {
		selectedUUID = ""
		generatedIDEntry.SetText("")
		lastNameEntry.SetText("")
		firstNameEntry.SetText("")
		middleNameEntry.SetText("")
		extensionNameEntry.SetText("")
		if err := applyPSGCSelection("", "", "", "", "", "", "", ""); err != nil {
			runtime.SetStatus("PSGC reset failed")
			runtime.SetActivity(err.Error())
		}
		contactNoEntry.SetText("")
		birthMonthEntry.SetText("")
		birthDayEntry.SetText("")
		birthYearEntry.SetText("")
		sexEntry.SetText("")
		duplicateLabel.SetText("Precheck ready")
	}

	setForm := func(item model.Beneficiary) {
		selectedUUID = item.InternalUUID
		generatedIDEntry.SetText(item.GeneratedID)
		lastNameEntry.SetText(item.LastName)
		firstNameEntry.SetText(item.FirstName)
		middleNameEntry.SetText(derefString(item.MiddleName))
		extensionNameEntry.SetText(derefString(item.ExtensionName))
		if err := applyPSGCSelection(item.RegionCode, item.RegionName, item.ProvinceCode, item.ProvinceName, item.CityCode, item.CityName, item.BarangayCode, item.BarangayName); err != nil {
			runtime.SetStatus("PSGC preload failed")
			runtime.SetActivity(err.Error())
		}
		contactNoEntry.SetText(derefString(item.ContactNo))
		populateBirthdateFields(birthMonthEntry, birthDayEntry, birthYearEntry, item)
		sexEntry.SetText(item.Sex)
		duplicateLabel.SetText(fmt.Sprintf("Selected: %s | %s, %s (%s)", item.GeneratedID, item.LastName, item.FirstName, item.RecordStatus))
	}

	beneficiaryList := widget.NewList(
		func() int {
			return len(items)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(index widget.ListItemID, object fyne.CanvasObject) {
			if index < 0 || index >= len(items) {
				object.(*widget.Label).SetText("")
				return
			}
			item := items[index]
			object.(*widget.Label).SetText(formatBeneficiaryListItem(item))
		},
	)
	beneficiaryList.OnSelected = func(id widget.ListItemID) {
		if id < 0 || id >= len(items) {
			return
		}
		setForm(items[id])
		runtime.SetActivity("Loaded beneficiary into form")
	}

	emptyStateLabel := widget.NewLabel("No beneficiaries loaded yet.\nUse New to create the first record or Import to bring one in.")
	emptyStateLabel.Alignment = fyne.TextAlignCenter
	emptyStateLabel.Wrapping = fyne.TextWrapWord
	emptyStatePanel := container.NewCenter(container.NewVBox(emptyStateLabel))
	beneficiaryListShell := container.NewStack(emptyStatePanel, container.NewVScroll(beneficiaryList))

	refreshList := func(search string, preserveSelection string) {
		search = strings.TrimSpace(search)
		preserveSelection = strings.TrimSpace(preserveSelection)
		runtime.RunAsync("Loading beneficiaries", func() error {
			page, err := runtime.Repository.ListBeneficiaries(context.Background(), repository.BeneficiaryListQuery{
				Search:         search,
				IncludeDeleted: true,
				Limit:          300,
				Offset:         0,
			})
			if err != nil {
				return err
			}

			fyne.Do(func() {
				items = page.Items
				resultLabel.SetText(fmt.Sprintf("Results: %d", page.Total))
				beneficiaryList.Refresh()
				if page.Total == 0 {
					if strings.TrimSpace(search) == "" {
						emptyStateLabel.SetText("No beneficiaries loaded yet.\nUse New to create the first record or Import to bring one in.")
					} else {
						emptyStateLabel.SetText("No matching beneficiaries found.\nTry a different search or clear the filter.")
					}
					emptyStatePanel.Show()
				} else {
					emptyStatePanel.Hide()
				}
				if preserveSelection != "" {
					index := findBeneficiaryIndexByUUID(items, preserveSelection)
					if index >= 0 {
						beneficiaryList.Select(index)
					}
				}
			})

			return nil
		})
	}

	collectValues := func() beneficiaryFormValues {
		return beneficiaryFormValues{
			GeneratedID:   generatedIDEntry.Text,
			LastName:      lastNameEntry.Text,
			FirstName:     firstNameEntry.Text,
			MiddleName:    middleNameEntry.Text,
			ExtensionName: extensionNameEntry.Text,
			RegionCode:    psgcState.RegionCode,
			RegionName:    psgcState.RegionName,
			ProvinceCode:  psgcState.ProvinceCode,
			ProvinceName:  psgcState.ProvinceName,
			CityCode:      psgcState.CityCode,
			CityName:      psgcState.CityName,
			BarangayCode:  psgcState.BarangayCode,
			BarangayName:  psgcState.BarangayName,
			ContactNo:     contactNoEntry.Text,
			BirthMonth:    birthMonthEntry.Text,
			BirthDay:      birthDayEntry.Text,
			BirthYear:     birthYearEntry.Text,
			Sex:           sexEntry.Text,
		}
	}

	runPrecheck := func(values beneficiaryFormValues) {
		draft, err := toBeneficiaryDraft(values)
		if err != nil {
			runtime.SetStatus("Duplicate precheck input error")
			runtime.SetActivity(err.Error())
			return
		}
		currentSelectedUUID := strings.TrimSpace(selectedUUID)
		runtime.RunAsync("Running duplicate precheck", func() error {
			prompt, err := runtime.BeneficiaryService.BuildDuplicatePrecheckPrompt(context.Background(), draft, currentSelectedUUID)
			if err != nil {
				return err
			}
			fyne.Do(func() {
				duplicateLabel.SetText(summarizeDuplicatePrompt(prompt))
			})
			return nil
		})
	}

	saveBeneficiary := func(values beneficiaryFormValues) {
		draft, err := toBeneficiaryDraft(values)
		if err != nil {
			runtime.SetStatus("Save input error")
			runtime.SetActivity(err.Error())
			return
		}
		search := searchEntry.Text
		currentSelectedUUID := strings.TrimSpace(selectedUUID)
		runtime.RunAsync("Saving beneficiary", func() error {
			nextSelectedUUID := currentSelectedUUID
			prompt, err := runtime.BeneficiaryService.BuildDuplicatePrecheckPrompt(context.Background(), draft, currentSelectedUUID)
			if err != nil {
				return err
			}
			if prompt != nil && prompt.RequiresConfirmation {
				confirmed, err := confirmOnUI(runtime.Window, "Exact duplicate detected", summarizeDuplicatePrompt(prompt)+"\n\nSave anyway?")
				if err != nil {
					return err
				}
				if !confirmed {
					fyne.Do(func() {
						duplicateLabel.SetText("Save cancelled by user")
					})
					return nil
				}
			}

			if currentSelectedUUID == "" {
				created, err := runtime.BeneficiaryService.CreateBeneficiary(context.Background(), draft, service.CreateOptions{
					PreferredGeneratedID: strings.TrimSpace(values.GeneratedID),
					SourceType:           model.BeneficiarySourceLocal,
					RecordStatus:         model.RecordStatusActive,
					DedupStatus:          model.DedupStatusClear,
				})
				if err != nil {
					return err
				}
				if created != nil {
					createdUUID := strings.TrimSpace(created.InternalUUID)
					if createdUUID != "" {
						nextSelectedUUID = createdUUID
						fyne.Do(func() {
							selectedUUID = createdUUID
						})
					}
				}
			} else {
				existing, err := runtime.Repository.GetBeneficiary(context.Background(), currentSelectedUUID)
				if err != nil {
					return err
				}
				normalized, err := runtime.BeneficiaryService.NormalizeAndValidateDraft(draft)
				if err != nil {
					return err
				}

				existing.LastName = normalized.LastName
				existing.FirstName = normalized.FirstName
				existing.MiddleName = normalized.MiddleName
				existing.ExtensionName = normalized.ExtensionName
				existing.NormLastName = normalized.NormLastName
				existing.NormFirstName = normalized.NormFirstName
				existing.NormMiddleName = normalized.NormMiddleName
				existing.NormExtensionName = normalized.NormExtensionName
				existing.RegionCode = normalized.RegionCode
				existing.RegionName = normalized.RegionName
				existing.ProvinceCode = normalized.ProvinceCode
				existing.ProvinceName = normalized.ProvinceName
				existing.CityCode = normalized.CityCode
				existing.CityName = normalized.CityName
				existing.BarangayCode = normalized.BarangayCode
				existing.BarangayName = normalized.BarangayName
				existing.ContactNo = normalized.ContactNo
				existing.ContactNoNorm = normalized.ContactNoNorm
				existing.BirthMonth = normalized.BirthMonth
				existing.BirthDay = normalized.BirthDay
				existing.BirthYear = normalized.BirthYear
				existing.BirthdateISO = normalized.BirthdateISO
				existing.Sex = normalized.Sex
				existing.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)

				if err := runtime.Repository.UpdateBeneficiary(context.Background(), existing); err != nil {
					return err
				}
			}

			fyne.Do(func() {
				duplicateLabel.SetText("Beneficiary saved")
			})
			refreshList(search, nextSelectedUUID)
			return nil
		})
	}

	softDeleteSelected := func() {
		if strings.TrimSpace(selectedUUID) == "" {
			runtime.SetStatus("Select a beneficiary to soft delete")
			return
		}

		go func(id string, search string) {
			confirmed, err := confirmOnUI(runtime.Window, "Soft delete beneficiary", "This will mark the selected beneficiary as DELETED. Continue?")
			if err != nil {
				runtime.SetStatus("Error: " + err.Error())
				return
			}
			if !confirmed {
				runtime.SetStatus("Soft delete cancelled")
				return
			}
			runtime.RunAsync("Soft deleting beneficiary", func() error {
				if err := runtime.BeneficiaryService.SoftDeleteBeneficiary(context.Background(), id); err != nil {
					return err
				}
				fyne.Do(func() {
					clearForm()
				})
				refreshList(search, "")
				return nil
			})
		}(selectedUUID, searchEntry.Text)
	}

	searchBtn := widget.NewButton("Search", func() {
		refreshList(searchEntry.Text, strings.TrimSpace(selectedUUID))
	})
	clearSearchBtn := widget.NewButton("Clear", func() {
		searchEntry.SetText("")
		refreshList("", strings.TrimSpace(selectedUUID))
	})
	newBtn := widget.NewButton("New", func() {
		clearForm()
		beneficiaryList.UnselectAll()
		runtime.SetActivity("New beneficiary form ready")
	})
	precheckBtn := widget.NewButton("Precheck", func() {
		runPrecheck(collectValues())
	})
	saveBtn := widget.NewButton("Save", func() {
		saveBeneficiary(collectValues())
	})
	softDeleteBtn := widget.NewButton("Soft Delete", softDeleteSelected)
	reloadBtn := widget.NewButton("Reload", func() {
		refreshList(searchEntry.Text, strings.TrimSpace(selectedUUID))
	})

	precheckBtn.Importance = widget.MediumImportance
	saveBtn.Importance = widget.HighImportance
	newBtn.Importance = widget.LowImportance
	softDeleteBtn.Importance = widget.LowImportance
	reloadBtn.Importance = widget.LowImportance

	leftPane := container.NewBorder(
		container.NewVBox(
			widget.NewLabel("Search"),
			searchEntry,
			container.NewHBox(searchBtn, clearSearchBtn),
			resultLabel,
		),
		nil,
		nil,
		nil,
		beneficiaryListShell,
	)

	identityCard := SectionCard(
		"Identity",
		"Core beneficiary profile",
		container.NewGridWithColumns(2,
			labeledField("Generated ID", generatedIDEntry),
			labeledField("Sex", sexEntry),
			labeledField("Last Name", lastNameEntry),
			labeledField("First Name", firstNameEntry),
			labeledField("Middle Name", middleNameEntry),
			labeledField("Extension", extensionNameEntry),
		),
	)

	locationCard := SectionCard(
		"PSGC Location",
		"Choose a region first; the remaining codes resolve automatically.",
		container.NewVBox(
			labeledField("Region", container.NewVBox(regionSelect, container.NewHBox(regionCodeValue, layout.NewSpacer(), regionNameValue))),
			labeledField("Province", container.NewVBox(provinceSelect, container.NewHBox(provinceCodeValue, layout.NewSpacer(), provinceNameValue))),
			labeledField("City", container.NewVBox(citySelect, container.NewHBox(cityCodeValue, layout.NewSpacer(), cityNameValue))),
			labeledField("Barangay", container.NewVBox(barangaySelect, container.NewHBox(barangayCodeValue, layout.NewSpacer(), barangayNameValue))),
		),
	)

	contactCard := SectionCard(
		"Contact & Birthdate",
		"Split birthdate fields keep the CSV template and PSGC matching operator-friendly.",
		container.NewGridWithColumns(2,
			labeledField("Birth Month", birthMonthEntry),
			labeledField("Birth Day", birthDayEntry),
			labeledField("Birth Year", birthYearEntry),
			labeledField("Contact No", contactNoEntry),
		),
	)

	actionBar := container.NewHBox(newBtn, precheckBtn, saveBtn, softDeleteBtn, layout.NewSpacer(), reloadBtn)

	if err := ensureRegionsLoaded(); err != nil {
		runtime.SetStatus("PSGC region load failed")
		runtime.SetActivity(err.Error())
	}

	rightPane := container.NewBorder(
		container.NewVBox(
			SectionHeader("Beneficiary Encoding", "PSGC-backed fields resolve the region chain and codes automatically."),
			duplicateLabel,
		),
		actionBar,
		nil,
		nil,
		container.NewVScroll(container.NewVBox(
			container.NewAdaptiveGrid(2, identityCard, locationCard),
			contactCard,
		)),
	)

	split := container.NewHSplit(leftPane, rightPane)
	split.Offset = 0.42

	clearForm()
	refreshList("", "")

	return split
}

func labeledField(label string, input fyne.CanvasObject) fyne.CanvasObject {
	return container.NewVBox(widget.NewLabel(label), input)
}

func toBeneficiaryDraft(values beneficiaryFormValues) (service.BeneficiaryDraft, error) {
	birthMonth, err := parseOptionalInt64Text(values.BirthMonth, "Birth Month")
	if err != nil {
		return service.BeneficiaryDraft{}, err
	}
	birthDay, err := parseOptionalInt64Text(values.BirthDay, "Birth Day")
	if err != nil {
		return service.BeneficiaryDraft{}, err
	}
	birthYear, err := parseOptionalInt64Text(values.BirthYear, "Birth Year")
	if err != nil {
		return service.BeneficiaryDraft{}, err
	}

	return service.BeneficiaryDraft{
		LastName:      strings.TrimSpace(values.LastName),
		FirstName:     strings.TrimSpace(values.FirstName),
		MiddleName:    strings.TrimSpace(values.MiddleName),
		ExtensionName: strings.TrimSpace(values.ExtensionName),
		RegionCode:    strings.TrimSpace(values.RegionCode),
		RegionName:    strings.TrimSpace(values.RegionName),
		ProvinceCode:  strings.TrimSpace(values.ProvinceCode),
		ProvinceName:  strings.TrimSpace(values.ProvinceName),
		CityCode:      strings.TrimSpace(values.CityCode),
		CityName:      strings.TrimSpace(values.CityName),
		BarangayCode:  strings.TrimSpace(values.BarangayCode),
		BarangayName:  strings.TrimSpace(values.BarangayName),
		ContactNo:     strings.TrimSpace(values.ContactNo),
		BirthMonth:    birthMonth,
		BirthDay:      birthDay,
		BirthYear:     birthYear,
		Sex:           strings.TrimSpace(values.Sex),
	}, nil
}

func summarizeDuplicatePrompt(prompt *service.DuplicatePrecheckPrompt) string {
	if prompt == nil {
		return "Duplicate precheck: no response"
	}
	if prompt.HasExactDuplicate {
		return fmt.Sprintf("Exact duplicates: %d | Candidates: %d | %s", len(prompt.ExactDuplicates), len(prompt.Candidates), strings.TrimSpace(prompt.Message))
	}
	if len(prompt.Candidates) > 0 {
		return fmt.Sprintf("Candidates: %d | %s", len(prompt.Candidates), strings.TrimSpace(prompt.Message))
	}
	message := strings.TrimSpace(prompt.Message)
	if message == "" {
		message = "No duplicates found"
	}
	return message
}

func formatBeneficiaryListItem(item model.Beneficiary) string {
	birthdate := formatBeneficiaryBirthdate(item)
	if birthdate == "" {
		birthdate = "n/a"
	}
	return fmt.Sprintf("%s | %s, %s | %s/%s | %s", strings.TrimSpace(item.GeneratedID), item.LastName, item.FirstName, item.RecordStatus, item.DedupStatus, birthdate)
}

func populateBirthdateFields(monthEntry, dayEntry, yearEntry *widget.Entry, item model.Beneficiary) {
	if monthEntry == nil || dayEntry == nil || yearEntry == nil {
		return
	}

	if item.BirthMonth != nil && item.BirthDay != nil && item.BirthYear != nil {
		monthEntry.SetText(fmt.Sprintf("%02d", *item.BirthMonth))
		dayEntry.SetText(fmt.Sprintf("%02d", *item.BirthDay))
		yearEntry.SetText(fmt.Sprintf("%04d", *item.BirthYear))
		return
	}

	month, day, year, ok := parseBirthdateISO(derefString(item.BirthdateISO))
	if !ok {
		monthEntry.SetText("")
		dayEntry.SetText("")
		yearEntry.SetText("")
		return
	}

	monthEntry.SetText(fmt.Sprintf("%02d", month))
	dayEntry.SetText(fmt.Sprintf("%02d", day))
	yearEntry.SetText(fmt.Sprintf("%04d", year))
}

func formatBeneficiaryBirthdate(item model.Beneficiary) string {
	if item.BirthMonth != nil && item.BirthDay != nil && item.BirthYear != nil {
		return fmt.Sprintf("%04d-%02d-%02d", *item.BirthYear, *item.BirthMonth, *item.BirthDay)
	}
	return derefString(item.BirthdateISO)
}

func parseBirthdateISO(value string) (int64, int64, int64, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, 0, 0, false
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return 0, 0, 0, false
	}
	return int64(parsed.Month()), int64(parsed.Day()), int64(parsed.Year()), true
}

func parseOptionalInt64Text(value, label string) (*int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be numeric", label)
	}
	return &parsed, nil
}

func findBeneficiaryIndexByUUID(items []model.Beneficiary, internalUUID string) int {
	internalUUID = strings.TrimSpace(internalUUID)
	if internalUUID == "" {
		return -1
	}
	for index := range items {
		if items[index].InternalUUID == internalUUID {
			return index
		}
	}
	return -1
}

func confirmOnUI(win fyne.Window, title, message string) (bool, error) {
	if win == nil {
		return false, fmt.Errorf("window is nil")
	}
	result := make(chan bool, 1)
	fyne.Do(func() {
		dialog.ShowConfirm(title, message, func(ok bool) {
			result <- ok
		}, win)
	})
	return <-result, nil
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func parseOptionalInt64(value string) (*int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
