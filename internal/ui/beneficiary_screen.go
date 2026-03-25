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
	BirthdateISO  string
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

	searchEntry := widget.NewEntry()
	searchEntry.SetPlaceHolder("Search by generated ID, name, contact, or source reference")
	resultLabel := widget.NewLabel("Results: 0")
	duplicateLabel := widget.NewLabel("Duplicate precheck: not run")

	generatedIDEntry := widget.NewEntry()
	generatedIDEntry.SetPlaceHolder("Optional - leave blank for auto ID")
	lastNameEntry := widget.NewEntry()
	firstNameEntry := widget.NewEntry()
	middleNameEntry := widget.NewEntry()
	extensionNameEntry := widget.NewEntry()
	regionCodeEntry := widget.NewEntry()
	regionNameEntry := widget.NewEntry()
	provinceCodeEntry := widget.NewEntry()
	provinceNameEntry := widget.NewEntry()
	cityCodeEntry := widget.NewEntry()
	cityNameEntry := widget.NewEntry()
	barangayCodeEntry := widget.NewEntry()
	barangayNameEntry := widget.NewEntry()
	contactNoEntry := widget.NewEntry()
	birthdateEntry := widget.NewEntry()
	birthdateEntry.SetPlaceHolder("YYYY-MM-DD or blank")
	sexEntry := widget.NewEntry()
	sexEntry.SetPlaceHolder("M/F")

	clearForm := func() {
		selectedUUID = ""
		generatedIDEntry.SetText("")
		lastNameEntry.SetText("")
		firstNameEntry.SetText("")
		middleNameEntry.SetText("")
		extensionNameEntry.SetText("")
		regionCodeEntry.SetText("")
		regionNameEntry.SetText("")
		provinceCodeEntry.SetText("")
		provinceNameEntry.SetText("")
		cityCodeEntry.SetText("")
		cityNameEntry.SetText("")
		barangayCodeEntry.SetText("")
		barangayNameEntry.SetText("")
		contactNoEntry.SetText("")
		birthdateEntry.SetText("")
		sexEntry.SetText("")
		duplicateLabel.SetText("Duplicate precheck: not run")
	}

	setForm := func(item model.Beneficiary) {
		selectedUUID = item.InternalUUID
		generatedIDEntry.SetText(item.GeneratedID)
		lastNameEntry.SetText(item.LastName)
		firstNameEntry.SetText(item.FirstName)
		middleNameEntry.SetText(derefString(item.MiddleName))
		extensionNameEntry.SetText(derefString(item.ExtensionName))
		regionCodeEntry.SetText(item.RegionCode)
		regionNameEntry.SetText(item.RegionName)
		provinceCodeEntry.SetText(item.ProvinceCode)
		provinceNameEntry.SetText(item.ProvinceName)
		cityCodeEntry.SetText(item.CityCode)
		cityNameEntry.SetText(item.CityName)
		barangayCodeEntry.SetText(item.BarangayCode)
		barangayNameEntry.SetText(item.BarangayName)
		contactNoEntry.SetText(derefString(item.ContactNo))
		birthdateEntry.SetText(derefString(item.BirthdateISO))
		sexEntry.SetText(item.Sex)
		duplicateLabel.SetText(fmt.Sprintf("Selected: %s | %s, %s (%s)", item.GeneratedID, item.LastName, item.FirstName, item.RecordStatus))
	}

	beneficiaryList := widget.NewList(
		func() int {
			return len(items)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("template")
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
			RegionCode:    regionCodeEntry.Text,
			RegionName:    regionNameEntry.Text,
			ProvinceCode:  provinceCodeEntry.Text,
			ProvinceName:  provinceNameEntry.Text,
			CityCode:      cityCodeEntry.Text,
			CityName:      cityNameEntry.Text,
			BarangayCode:  barangayCodeEntry.Text,
			BarangayName:  barangayNameEntry.Text,
			ContactNo:     contactNoEntry.Text,
			BirthdateISO:  birthdateEntry.Text,
			Sex:           sexEntry.Text,
		}
	}

	runPrecheck := func(values beneficiaryFormValues) {
		draft := toBeneficiaryDraft(values)
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
		draft := toBeneficiaryDraft(values)
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
		container.NewVScroll(beneficiaryList),
	)

	formGrid := container.NewAdaptiveGrid(2,
		labeledField("Generated ID", generatedIDEntry),
		labeledField("Sex", sexEntry),
		labeledField("Last Name", lastNameEntry),
		labeledField("First Name", firstNameEntry),
		labeledField("Middle Name", middleNameEntry),
		labeledField("Extension", extensionNameEntry),
		labeledField("Birthdate ISO", birthdateEntry),
		labeledField("Contact No", contactNoEntry),
		labeledField("Region Code", regionCodeEntry),
		labeledField("Region Name", regionNameEntry),
		labeledField("Province Code", provinceCodeEntry),
		labeledField("Province Name", provinceNameEntry),
		labeledField("City Code", cityCodeEntry),
		labeledField("City Name", cityNameEntry),
		labeledField("Barangay Code", barangayCodeEntry),
		labeledField("Barangay Name", barangayNameEntry),
	)

	actionBar := container.NewHBox(newBtn, precheckBtn, saveBtn, softDeleteBtn, layout.NewSpacer(), reloadBtn)
	rightPane := container.NewBorder(
		container.NewVBox(widget.NewLabel("Beneficiary Encoding"), duplicateLabel),
		actionBar,
		nil,
		nil,
		container.NewVScroll(formGrid),
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

func toBeneficiaryDraft(values beneficiaryFormValues) service.BeneficiaryDraft {
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
		BirthdateISO:  strings.TrimSpace(values.BirthdateISO),
		Sex:           strings.TrimSpace(values.Sex),
	}
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
	birthdate := derefString(item.BirthdateISO)
	if birthdate == "" {
		birthdate = "n/a"
	}
	return fmt.Sprintf("%s | %s, %s | %s/%s | %s", strings.TrimSpace(item.GeneratedID), item.LastName, item.FirstName, item.RecordStatus, item.DedupStatus, birthdate)
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
