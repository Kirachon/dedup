package ui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"dedup/internal/dedup"
	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

func init() {
	registerScreen(20, "Dedup", buildDedupScreen)
}

func buildDedupScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Dedup screen unavailable")
	}

	var (
		runs             []model.DedupRun
		matches          []model.DedupMatch
		selectedRunID    string
		selectedMatchID  string
		reviewWindow     fyne.Window
		openReviewWindow func(string)
	)

	runsList := widget.NewList(
		func() int { return len(runs) },
		func() fyne.CanvasObject { return widget.NewLabel("") },
		func(index widget.ListItemID, object fyne.CanvasObject) {
			if index < 0 || index >= len(runs) {
				object.(*widget.Label).SetText("")
				return
			}
			object.(*widget.Label).SetText(formatDedupRunItem(runs[index]))
		},
	)

	matchesList := widget.NewList(
		func() int { return len(matches) },
		func() fyne.CanvasObject { return widget.NewLabel("") },
		func(index widget.ListItemID, object fyne.CanvasObject) {
			if index < 0 || index >= len(matches) {
				object.(*widget.Label).SetText("")
				return
			}
			object.(*widget.Label).SetText(formatDedupMatchItem(matches[index]))
		},
	)

	thresholdEntry := widget.NewEntry()
	thresholdEntry.SetText("90")
	initiatedByEntry := widget.NewEntry()
	initiatedByEntry.SetText("operator")
	includeDeletedCheck := widget.NewCheck("Include DELETED beneficiaries", nil)
	decisionSelect := widget.NewSelect([]string{
		string(model.DedupDecisionRetainA),
		string(model.DedupDecisionRetainB),
		string(model.DedupDecisionRetainBoth),
		string(model.DedupDecisionDeleteASoft),
		string(model.DedupDecisionDeleteBSoft),
		string(model.DedupDecisionDifferent),
	}, nil)
	decisionSelect.SetSelected(string(model.DedupDecisionRetainBoth))
	resolvedByEntry := widget.NewEntry()
	resolvedByEntry.SetText("reviewer")
	notesEntry := widget.NewMultiLineEntry()
	notesEntry.SetPlaceHolder("Add decision notes if needed")
	summaryLabel := widget.NewLabel("Select a match to preview both beneficiary records")
	summaryLabel.Wrapping = fyne.TextWrapWord
	queueStatusLabel := widget.NewLabel("No candidate selected yet")
	queueStatusLabel.Wrapping = fyne.TextWrapWord

	reviewBody := container.NewVBox(buildDedupMatchReviewEmptyState("Select a match to preview both beneficiary records"))
	setReviewBody := func(obj fyne.CanvasObject) {
		fyne.Do(func() {
			reviewBody.Objects = []fyne.CanvasObject{obj}
			reviewBody.Refresh()
		})
	}

	loadSelectedMatchReview := func(matchID string) {
		matchID = strings.TrimSpace(matchID)
		selectedMatchID = matchID
		if matchID == "" {
			fyne.Do(func() {
				setReviewBody(buildDedupMatchReviewEmptyState("Select a match to preview both beneficiary records"))
			})
			return
		}

		index := findDedupMatchIndexByID(matches, matchID)
		if index < 0 {
			fyne.Do(func() {
				summaryLabel.SetText("Selected dedup match is no longer available")
				setReviewBody(buildDedupMatchReviewEmptyState("Selected dedup match is no longer available"))
			})
			return
		}

		match := matches[index]
		fyne.Do(func() {
			summaryLabel.SetText(fmt.Sprintf("%s | score=%.2f | %s", match.MatchID, match.TotalScore, match.DecisionStatus))
			setReviewBody(buildDedupMatchReviewLoadingState("Loading match details..."))
		})

		runtime.RunAsync("Loading dedup match review", func() error {
			recordA, errA := runtime.Repository.GetBeneficiary(context.Background(), match.RecordAUUID)
			recordB, errB := runtime.Repository.GetBeneficiary(context.Background(), match.RecordBUUID)
			fyne.Do(func() {
				if strings.TrimSpace(selectedMatchID) != matchID {
					return
				}
				summaryLabel.SetText(fmt.Sprintf("%s | score=%.2f | %s", match.MatchID, match.TotalScore, match.DecisionStatus))
				setReviewBody(buildDedupMatchReviewContent(match, recordA, errA, recordB, errB))
			})
			return nil
		})
	}

	refreshMatches := func(runID string) {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			fyne.Do(func() {
				matches = nil
				matchesList.Refresh()
				summaryLabel.SetText("Select a match to preview both beneficiary records")
				queueStatusLabel.SetText("No run selected")
				loadSelectedMatchReview("")
				if openReviewWindow != nil {
					openReviewWindow("")
				}
			})
			return
		}

		runtime.RunAsync("Loading dedup matches", func() error {
			items, err := runtime.Repository.ListDedupMatchesByRun(context.Background(), runID)
			if err != nil {
				return err
			}
			fyne.Do(func() {
				matches = items
				matchesList.Refresh()
				if selectedMatchID != "" {
					index := findDedupMatchIndexByID(matches, selectedMatchID)
					if index >= 0 {
						matchesList.Select(index)
					} else {
						loadSelectedMatchReview("")
					}
				} else {
					if len(matches) > 0 {
						matchesList.Select(0)
						return
					}
					loadSelectedMatchReview("")
					queueStatusLabel.SetText("No match candidates available")
					if openReviewWindow != nil {
						openReviewWindow("")
					}
				}
			})
			return nil
		})
	}

	refreshRuns := func() {
		runtime.RunAsync("Loading dedup runs", func() error {
			items, err := runtime.Repository.ListDedupRuns(context.Background(), repository.DedupRunListQuery{Limit: 250, Offset: 0})
			if err != nil {
				return err
			}
			fyne.Do(func() {
				runs = items
				runsList.Refresh()
				if selectedRunID != "" {
					index := findDedupRunIndexByID(runs, selectedRunID)
					if index >= 0 {
						runsList.Select(index)
					}
				}
			})
			return nil
		})
	}

	runsList.OnSelected = func(id widget.ListItemID) {
		if id < 0 || id >= len(runs) {
			return
		}
		selectedRunID = runs[id].RunID
		selectedMatchID = ""
		loadSelectedMatchReview("")
		if openReviewWindow != nil {
			openReviewWindow("")
		}
		queueStatusLabel.SetText(fmt.Sprintf("Run selected: %s", compactDedupRunID(selectedRunID)))
		summaryLabel.SetText(fmt.Sprintf("Run %s selected", selectedRunID))
		refreshMatches(selectedRunID)
	}

	matchesList.OnSelected = func(id widget.ListItemID) {
		if id < 0 || id >= len(matches) {
			return
		}
		loadSelectedMatchReview(matches[id].MatchID)
		if openReviewWindow != nil {
			openReviewWindow(matches[id].MatchID)
		}
		queueStatusLabel.SetText(fmt.Sprintf("Candidate selected: %s", compactDedupMatchID(matches[id].MatchID)))
	}

	runDedupBtn := widget.NewButton("Run Dedup", func() {
		thresholdText := thresholdEntry.Text
		initiatedBy := strings.TrimSpace(initiatedByEntry.Text)
		includeDeleted := includeDeletedCheck.Checked

		runtime.RunAsync("Running deterministic dedup", func() error {
			threshold, err := parseDedupThreshold(thresholdText)
			if err != nil {
				return err
			}
			beneficiaries, err := listAllBeneficiariesForDedup(runtime.Repository, includeDeleted)
			if err != nil {
				return err
			}
			runID := newDedupRunID()
			result, err := runtime.DedupEngine.Run(dedup.RunRequest{
				RunID:          runID,
				InitiatedBy:    initiatedBy,
				Threshold:      threshold,
				IncludeDeleted: includeDeleted,
			}, beneficiaries)
			if err != nil {
				return err
			}

			nowUTC := time.Now().UTC().Format(time.RFC3339Nano)
			notes := fmt.Sprintf("initiated_by=%s threshold=%.2f include_deleted=%t", normalizeActor(initiatedBy), threshold, includeDeleted)
			run := &model.DedupRun{
				RunID:           runID,
				StartedAt:       nowUTC,
				CompletedAt:     stringPtr(nowUTC),
				Status:          "succeeded",
				TotalCandidates: result.TotalCandidates,
				TotalMatches:    len(result.Matches),
				Notes:           stringPtr(notes),
			}
			if err := runtime.Repository.CreateDedupRun(context.Background(), run); err != nil {
				return err
			}

			for index, match := range result.Matches {
				record := &model.DedupMatch{
					MatchID:            fmt.Sprintf("%s-%05d", runID, index+1),
					RunID:              runID,
					RecordAUUID:        match.RecordAUUID,
					RecordBUUID:        match.RecordBUUID,
					PairKey:            match.PairKey,
					FirstNameScore:     match.FirstNameScore,
					MiddleNameScore:    match.MiddleNameScore,
					LastNameScore:      match.LastNameScore,
					ExtensionNameScore: match.ExtensionNameScore,
					TotalScore:         match.TotalScore,
					BirthdateCompare:   match.BirthdateCompare.NullableInt64Ptr(),
					BarangayCompare:    match.BarangayCompare.NullableInt64Ptr(),
					DecisionStatus:     "PENDING",
					CreatedAt:          nowUTC,
				}
				if err := runtime.Repository.CreateDedupMatch(context.Background(), record); err != nil {
					return err
				}
			}

			loadedRuns, err := runtime.Repository.ListDedupRuns(context.Background(), repository.DedupRunListQuery{Limit: 250, Offset: 0})
			if err != nil {
				return err
			}
			loadedMatches, err := runtime.Repository.ListDedupMatchesByRun(context.Background(), runID)
			if err != nil {
				return err
			}

			fyne.Do(func() {
				selectedRunID = runID
				selectedMatchID = ""
				runs = loadedRuns
				matches = loadedMatches
				runsList.Refresh()
				matchesList.Refresh()
				summaryLabel.SetText(fmt.Sprintf("Dedup run completed: %d candidates, %d matches", result.TotalCandidates, len(result.Matches)))
				if len(loadedMatches) > 0 {
					matchesList.Select(0)
				} else {
					loadSelectedMatchReview("")
					queueStatusLabel.SetText("No match candidates were generated")
					if openReviewWindow != nil {
						openReviewWindow("")
					}
				}
			})
			return nil
		})
	})

	applyDecisionBtn := widget.NewButton("Apply Decision", func() {
		if strings.TrimSpace(selectedMatchID) == "" {
			runtime.SetStatus("Select a dedup match first")
			return
		}
		decisionValue, err := parseDedupDecisionType(decisionSelect.Selected)
		if err != nil {
			runtime.SetStatus("Error: " + err.Error())
			return
		}
		resolvedBy := strings.TrimSpace(resolvedByEntry.Text)
		notes := notesEntry.Text
		runID := selectedRunID

		runtime.RunAsync("Applying dedup decision", func() error {
			_, err := runtime.DedupDecision.ApplyDecision(context.Background(), service.ApplyDedupDecisionRequest{
				MatchID:    selectedMatchID,
				Decision:   decisionValue,
				ResolvedBy: normalizeActor(resolvedBy),
				Notes:      strings.TrimSpace(notes),
			})
			if err != nil {
				return err
			}
			updated, err := runtime.Repository.ListDedupMatchesByRun(context.Background(), runID)
			if err != nil {
				return err
			}
			fyne.Do(func() {
				matches = updated
				matchesList.Refresh()
				summaryLabel.SetText("Dedup decision applied")
				loadSelectedMatchReview(selectedMatchID)
			})
			return nil
		})
	})

	resetDecisionBtn := widget.NewButton("Reset Decision", func() {
		if strings.TrimSpace(selectedMatchID) == "" {
			runtime.SetStatus("Select a dedup match first")
			return
		}
		resetBy := strings.TrimSpace(resolvedByEntry.Text)
		notes := notesEntry.Text
		runID := selectedRunID

		runtime.RunAsync("Resetting dedup decision", func() error {
			_, err := runtime.DedupDecision.ResetDecision(context.Background(), service.ResetDedupDecisionRequest{
				MatchID: selectedMatchID,
				ResetBy: normalizeActor(resetBy),
				Notes:   strings.TrimSpace(notes),
			})
			if err != nil {
				return err
			}
			updated, err := runtime.Repository.ListDedupMatchesByRun(context.Background(), runID)
			if err != nil {
				return err
			}
			fyne.Do(func() {
				matches = updated
				matchesList.Refresh()
				summaryLabel.SetText("Dedup decision reset")
				loadSelectedMatchReview(selectedMatchID)
			})
			return nil
		})
	})

	refreshBtn := widget.NewButton("Refresh", func() {
		refreshRuns()
		if strings.TrimSpace(selectedRunID) != "" {
			refreshMatches(selectedRunID)
		}
	})
	mainRefreshBtn := widget.NewButton("Refresh", func() {
		refreshRuns()
		if strings.TrimSpace(selectedRunID) != "" {
			refreshMatches(selectedRunID)
		}
	})

	// ── Dedup Controls Card ──────────────────────────────────────────
	runDedupBtn.Importance = widget.HighImportance

	controls := container.NewAdaptiveGrid(2,
		labeledField("Threshold (0-100)", thresholdEntry),
		labeledField("Initiated By", initiatedByEntry),
	)

	controlsCard := Card(container.NewVBox(
		SectionHeader("Deduplication Review", "Run candidate matching and resolve duplicate records"),
		widget.NewSeparator(),
		controls,
		includeDeletedCheck,
		container.NewHBox(runDedupBtn),
	))

	// ── Decision Panel ────────────────────────────────────────────────
	decisionPanel := container.NewAdaptiveGrid(2,
		labeledField("Decision", decisionSelect),
		labeledField("Resolved/Reset By", resolvedByEntry),
	)

	applyDecisionBtn.Importance = widget.HighImportance

	reviewHeader := container.NewVBox(
		widget.NewLabelWithStyle("Match Review", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		summaryLabel,
		widget.NewSeparator(),
	)

	reviewFooter := container.NewVBox(
		decisionPanel,
		labeledField("Notes", notesEntry),
		container.NewHBox(applyDecisionBtn, resetDecisionBtn, layout.NewSpacer(), refreshBtn),
	)

	reviewCard := Card(container.NewBorder(
		reviewHeader,
		reviewFooter,
		nil, nil,
		reviewBody,
	))

	openReviewWindow = func(matchID string) {
		matchID = strings.TrimSpace(matchID)
		if matchID == "" && reviewWindow == nil {
			return
		}
		if reviewWindow == nil {
			if runtime.App == nil {
				runtime.SetStatus("Review window unavailable")
				return
			}
			reviewWindow = runtime.App.NewWindow("Match Review")
			reviewWindow.Resize(fyne.NewSize(1320, 900))
			reviewWindow.CenterOnScreen()
			reviewWindow.SetOnClosed(func() {
				reviewWindow = nil
			})
		}

		title := "Match Review"
		if matchID != "" {
			title = fmt.Sprintf("Match Review - %s", compactDedupMatchID(matchID))
		}
		reviewWindow.SetTitle(title)
		reviewWindow.SetContent(container.NewPadded(container.NewVScroll(reviewCard)))
		reviewWindow.Show()
		reviewWindow.RequestFocus()
	}

	openReviewBtn := widget.NewButton("Open Match Review", func() {
		openReviewWindow(selectedMatchID)
	})
	openReviewBtn.Importance = widget.HighImportance

	matchActionCard := Card(container.NewVBox(
		SectionHeader("Match Queue", "Choose a candidate, then open the review window for the full comparison"),
		queueStatusLabel,
		container.NewHBox(openReviewBtn, mainRefreshBtn, layout.NewSpacer()),
	))

	// ── Left-side queue rail ────────────────────────────────────────
	runsCard := Card(container.NewBorder(
		container.NewVBox(
			widget.NewLabelWithStyle("Dedup Runs", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
			widget.NewSeparator(),
		),
		nil, nil, nil,
		container.NewVScroll(runsList),
	))

	matchesCard := Card(container.NewBorder(
		container.NewVBox(
			widget.NewLabelWithStyle("Match Candidates", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
			widget.NewSeparator(),
		),
		nil, nil, nil,
		container.NewVScroll(matchesList),
	))

	queueRail := container.NewVSplit(runsCard, matchesCard)
	queueRail.Offset = 0.36

	refreshRuns()

	return container.NewVScroll(container.NewVBox(
		controlsCard,
		matchActionCard,
		queueRail,
	))
}

func formatDedupRunItem(item model.DedupRun) string {
	return fmt.Sprintf("%s • %s • %d candidates • %d matches", compactDedupRunID(item.RunID), strings.ToLower(strings.TrimSpace(item.Status)), item.TotalCandidates, item.TotalMatches)
}

func formatDedupMatchItem(item model.DedupMatch) string {
	return fmt.Sprintf("%s • %.2f%% • %s", compactDedupMatchID(item.MatchID), item.TotalScore, strings.TrimSpace(item.DecisionStatus))
}

func formatDedupMatchPreview(item model.DedupMatch, recordA *model.Beneficiary, errA error, recordB *model.Beneficiary, errB error) string {
	var builder strings.Builder

	fmt.Fprintf(&builder, "Match ID: %s\n", strings.TrimSpace(item.MatchID))
	fmt.Fprintf(&builder, "Decision Status: %s\n", strings.TrimSpace(item.DecisionStatus))
	fmt.Fprintf(&builder, "Pair Key: %s\n", strings.TrimSpace(item.PairKey))
	fmt.Fprintf(&builder, "Total Score: %.2f\n", item.TotalScore)
	fmt.Fprintf(
		&builder,
		"Component Scores: first=%.2f middle=%.2f last=%.2f extension=%.2f\n",
		item.FirstNameScore,
		item.MiddleNameScore,
		item.LastNameScore,
		item.ExtensionNameScore,
	)
	if item.BirthdateCompare != nil {
		fmt.Fprintf(&builder, "Birthdate Compare: %d\n", *item.BirthdateCompare)
	}
	if item.BarangayCompare != nil {
		fmt.Fprintf(&builder, "Barangay Compare: %d\n", *item.BarangayCompare)
	}

	builder.WriteString("\nRecord A\n")
	builder.WriteString(formatDedupBeneficiaryPreview(recordA, errA))
	builder.WriteString("\n\nRecord B\n")
	builder.WriteString(formatDedupBeneficiaryPreview(recordB, errB))

	return strings.TrimSpace(builder.String())
}

func formatDedupBeneficiaryPreview(item *model.Beneficiary, lookupErr error) string {
	if item == nil {
		if lookupErr != nil {
			return fmt.Sprintf("Unavailable: %v", lookupErr)
		}
		return "Unavailable: record not found"
	}

	birthday := strings.TrimSpace(formatBeneficiaryBirthdate(*item))
	if birthday == "" {
		birthday = "n/a"
	}

	location := formatDedupLocation(item)
	name := formatDedupDisplayName(item)
	contact := strings.TrimSpace(derefString(item.ContactNo))
	if contact == "" {
		contact = "n/a"
	}
	sex := strings.TrimSpace(item.Sex)
	if sex == "" {
		sex = "n/a"
	}

	var builder strings.Builder
	fmt.Fprintf(&builder, "Generated ID: %s\n", fallbackDisplayValue(strings.TrimSpace(item.GeneratedID)))
	fmt.Fprintf(&builder, "Name: %s\n", name)
	fmt.Fprintf(&builder, "Location:\n%s\n", indentText(location, "  "))
	fmt.Fprintf(&builder, "Birthday: %s\n", birthday)
	fmt.Fprintf(&builder, "Sex: %s\n", sex)
	fmt.Fprintf(&builder, "Contact No: %s", contact)
	return strings.TrimSpace(builder.String())
}

func formatDedupLocation(item *model.Beneficiary) string {
	if item == nil {
		return "  Region: n/a\n  Province: n/a\n  City/Municipality: n/a\n  Barangay: n/a"
	}

	lines := []string{
		fmt.Sprintf("Region: %s", formatDedupLocationValue(item.RegionName, item.RegionCode)),
		fmt.Sprintf("Province: %s", formatDedupLocationValue(item.ProvinceName, item.ProvinceCode)),
		fmt.Sprintf("City/Municipality: %s", formatDedupLocationValue(item.CityName, item.CityCode)),
		fmt.Sprintf("Barangay: %s", formatDedupLocationValue(item.BarangayName, item.BarangayCode)),
	}
	return strings.Join(lines, "\n")
}

func formatDedupLocationValue(name, code string) string {
	name = strings.TrimSpace(name)
	code = strings.TrimSpace(code)
	switch {
	case name != "" && code != "":
		return fmt.Sprintf("%s [%s]", name, code)
	case name != "":
		return name
	case code != "":
		return fmt.Sprintf("[%s]", code)
	default:
		return "n/a"
	}
}

func formatDedupDisplayName(item *model.Beneficiary) string {
	if item == nil {
		return "n/a"
	}

	parts := make([]string, 0, 4)
	if value := strings.TrimSpace(item.LastName); value != "" {
		parts = append(parts, value)
	}
	if value := strings.TrimSpace(item.FirstName); value != "" {
		if len(parts) == 0 {
			parts = append(parts, value)
		} else {
			parts[0] = parts[0] + ", " + value
		}
	}
	if value := strings.TrimSpace(derefString(item.MiddleName)); value != "" {
		parts = append(parts, value)
	}
	if value := strings.TrimSpace(derefString(item.ExtensionName)); value != "" {
		parts = append(parts, value)
	}
	if len(parts) == 0 {
		return "n/a"
	}
	return strings.Join(parts, " ")
}

func fallbackDisplayValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "n/a"
	}
	return value
}

func indentText(value, prefix string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return prefix + "n/a"
	}
	lines := strings.Split(value, "\n")
	for index, line := range lines {
		lines[index] = prefix + strings.TrimSpace(line)
	}
	return strings.Join(lines, "\n")
}

func buildDedupMatchReviewRecordCard(title string, item *model.Beneficiary, lookupErr error) fyne.CanvasObject {
	header := container.NewHBox(
		widget.NewLabelWithStyle(strings.ToUpper(strings.TrimSpace(title)), fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		layout.NewSpacer(),
	)

	if item == nil {
		message := "Record unavailable."
		if lookupErr != nil {
			message = fmt.Sprintf("Record unavailable: %v", lookupErr)
		}
		return Card(container.NewVBox(
			header,
			widget.NewSeparator(),
			widget.NewLabel(message),
		))
	}

	nameLabel := widget.NewLabelWithStyle(formatDedupDisplayName(item), fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	nameLabel.Wrapping = fyne.TextWrapWord

	idLabel := widget.NewLabelWithStyle(fallbackDisplayValue(strings.TrimSpace(item.GeneratedID)), fyne.TextAlignLeading, fyne.TextStyle{})
	idLabel.Wrapping = fyne.TextWrapWord

	birthday := fallbackDisplayValue(strings.TrimSpace(formatBeneficiaryBirthdate(*item)))
	sex := fallbackDisplayValue(strings.TrimSpace(item.Sex))
	contact := fallbackDisplayValue(strings.TrimSpace(derefString(item.ContactNo)))

	factGrid := container.NewAdaptiveGrid(2,
		buildDedupMatchFactCard("Generated ID", idLabel),
		buildDedupMatchFactCard("Birthday", widget.NewLabel(birthday)),
		buildDedupMatchFactCard("Region", widget.NewLabel(formatDedupLocationValue(item.RegionName, item.RegionCode))),
		buildDedupMatchFactCard("Province", widget.NewLabel(formatDedupLocationValue(item.ProvinceName, item.ProvinceCode))),
		buildDedupMatchFactCard("City/Municipality", widget.NewLabel(formatDedupLocationValue(item.CityName, item.CityCode))),
		buildDedupMatchFactCard("Barangay", widget.NewLabel(formatDedupLocationValue(item.BarangayName, item.BarangayCode))),
		buildDedupMatchFactCard("Sex", widget.NewLabel(sex)),
		buildDedupMatchFactCard("Contact No", widget.NewLabel(contact)),
	)

	return Card(container.NewVBox(
		header,
		widget.NewSeparator(),
		nameLabel,
		factGrid,
	))
}

func buildDedupMatchReviewEmptyState(message string) fyne.CanvasObject {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "Select a candidate from the list to compare the two beneficiary records."
	}
	label := widget.NewLabel(message)
	label.Wrapping = fyne.TextWrapWord
	return Card(container.NewVBox(
		widget.NewLabelWithStyle("No Match Selected", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewSeparator(),
		label,
	))
}

func buildDedupMatchReviewLoadingState(message string) fyne.CanvasObject {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "Loading match details..."
	}
	label := widget.NewLabel(message)
	label.Wrapping = fyne.TextWrapWord
	return Card(container.NewVBox(
		widget.NewLabelWithStyle("Loading Review", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewSeparator(),
		label,
	))
}

func buildDedupMatchReviewContent(match model.DedupMatch, recordA *model.Beneficiary, errA error, recordB *model.Beneficiary, errB error) fyne.CanvasObject {
	scoreText := fmt.Sprintf("%.0f%%", match.TotalScore)
	reasonText := fmt.Sprintf(
		"Name similarity: first %.2f, middle %.2f, last %.2f, extension %.2f. Birthdate and barangay signals are shown below.",
		match.FirstNameScore,
		match.MiddleNameScore,
		match.LastNameScore,
		match.ExtensionNameScore,
	)

	scoreCard := Card(container.NewVBox(
		widget.NewLabelWithStyle("MATCH SCORE", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("How strongly the two records resemble each other."),
		widget.NewSeparator(),
		container.NewCenter(MetricValueLabel(scoreText)),
		container.NewCenter(StatusBadgeForString(match.DecisionStatus)),
	))

	reasonLabel := widget.NewLabel(reasonText)
	reasonLabel.Wrapping = fyne.TextWrapWord
	reasonCard := Card(container.NewVBox(
		widget.NewLabelWithStyle("Similarity Notes", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewSeparator(),
		reasonLabel,
		buildDedupMatchFactCard("Pair Key", widget.NewLabel(fallbackDisplayValue(match.PairKey))),
	))

	recordGrid := container.NewAdaptiveGrid(2,
		buildDedupMatchReviewRecordCard("Record A", recordA, errA),
		buildDedupMatchReviewRecordCard("Record B", recordB, errB),
	)

	signalCards := container.NewAdaptiveGrid(3,
		buildDedupMatchSignalCard("Audit Trail", fmt.Sprintf("Match %s is currently %s.", compactDedupMatchID(match.MatchID), strings.ToLower(strings.TrimSpace(match.DecisionStatus)))),
		buildDedupMatchSignalCard("Location Signals", fmt.Sprintf("Birthdate compare: %s • Barangay compare: %s.", compareMetricText(match.BirthdateCompare), compareMetricText(match.BarangayCompare))),
		buildDedupMatchSignalCard("Decision Guidance", "Review the two cards above and choose which record should remain active."),
	)

	return container.NewVBox(
		container.NewAdaptiveGrid(2, scoreCard, reasonCard),
		recordGrid,
		signalCards,
	)
}

func buildDedupMatchFieldRow(label, value string) fyne.CanvasObject {
	value = fallbackDisplayValue(value)
	return container.NewVBox(
		widget.NewLabelWithStyle(label, fyne.TextAlignLeading, fyne.TextStyle{}),
		widget.NewLabelWithStyle(value, fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
	)
}

func buildDedupMatchFactCard(label string, value fyne.CanvasObject) fyne.CanvasObject {
	if value == nil {
		value = widget.NewLabel("n/a")
	}

	labelText := widget.NewLabelWithStyle(strings.ToUpper(strings.TrimSpace(label)), fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	labelText.Wrapping = fyne.TextTruncate
	return Card(container.NewVBox(
		labelText,
		value,
	))
}

func buildDedupMatchSignalCard(title, body string) fyne.CanvasObject {
	titleLabel := widget.NewLabelWithStyle(strings.TrimSpace(title), fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	titleLabel.Wrapping = fyne.TextWrapWord
	bodyLabel := widget.NewLabel(strings.TrimSpace(body))
	bodyLabel.Wrapping = fyne.TextWrapWord
	return Card(container.NewVBox(
		titleLabel,
		widget.NewSeparator(),
		bodyLabel,
	))
}

func compareMetricText(value *int64) string {
	if value == nil {
		return "n/a"
	}
	return fmt.Sprintf("%d", *value)
}

func compactDedupRunID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "n/a"
	}
	if len(value) <= 16 {
		return value
	}
	return value[:8] + "…" + value[len(value)-4:]
}

func compactDedupMatchID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "n/a"
	}
	if parts := strings.Split(value, "-"); len(parts) > 1 {
		tail := parts[len(parts)-1]
		if tail != "" {
			return tail
		}
	}
	return compactDedupRunID(value)
}

func parseDedupThreshold(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 90, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("threshold must be numeric")
	}
	if parsed < 0 || parsed > 100 {
		return 0, fmt.Errorf("threshold must be between 0 and 100")
	}
	return parsed, nil
}

func parseDedupDecisionType(value string) (model.DedupDecisionType, error) {
	decision := model.DedupDecisionType(strings.TrimSpace(value))
	switch decision {
	case model.DedupDecisionRetainA,
		model.DedupDecisionRetainB,
		model.DedupDecisionRetainBoth,
		model.DedupDecisionDeleteASoft,
		model.DedupDecisionDeleteBSoft,
		model.DedupDecisionDifferent:
		return decision, nil
	default:
		return "", fmt.Errorf("invalid dedup decision")
	}
}

func listAllBeneficiariesForDedup(repo *repository.Repository, includeDeleted bool) ([]model.Beneficiary, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is nil")
	}

	items := make([]model.Beneficiary, 0)
	offset := 0
	for {
		page, err := repo.ListBeneficiaries(context.Background(), repository.BeneficiaryListQuery{
			IncludeDeleted: includeDeleted,
			Limit:          500,
			Offset:         offset,
		})
		if err != nil {
			return nil, err
		}
		if len(page.Items) == 0 {
			break
		}
		items = append(items, page.Items...)
		offset += len(page.Items)
		if offset >= page.Total {
			break
		}
	}
	return items, nil
}

func newDedupRunID() string {
	return fmt.Sprintf("run-%d", time.Now().UTC().UnixNano())
}

func findDedupRunIndexByID(items []model.DedupRun, runID string) int {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return -1
	}
	for index := range items {
		if items[index].RunID == runID {
			return index
		}
	}
	return -1
}

func findDedupMatchIndexByID(items []model.DedupMatch, matchID string) int {
	matchID = strings.TrimSpace(matchID)
	if matchID == "" {
		return -1
	}
	for index := range items {
		if items[index].MatchID == matchID {
			return index
		}
	}
	return -1
}

func normalizeActor(actor string) string {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return "system"
	}
	return actor
}

func stringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
