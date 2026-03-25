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
		runs            []model.DedupRun
		matches         []model.DedupMatch
		selectedRunID   string
		selectedMatchID string
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
	summaryLabel := widget.NewLabel("Run a dedup pass to generate match candidates")

	refreshMatches := func(runID string) {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			fyne.Do(func() {
				matches = nil
				matchesList.Refresh()
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
		summaryLabel.SetText(fmt.Sprintf("Run %s selected", selectedRunID))
		refreshMatches(selectedRunID)
	}

	matchesList.OnSelected = func(id widget.ListItemID) {
		if id < 0 || id >= len(matches) {
			return
		}
		selectedMatchID = matches[id].MatchID
		summaryLabel.SetText(fmt.Sprintf("Match %s selected (%s)", matches[id].MatchID, matches[id].DecisionStatus))
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

	decisionCard := Card(container.NewVBox(
		summaryLabel,
		decisionPanel,
		labeledField("Notes", notesEntry),
		container.NewHBox(applyDecisionBtn, resetDecisionBtn, layout.NewSpacer(), refreshBtn),
	))

	// ── Two-panel split: Runs/Matches lists ────────────────────────
	left := Card(container.NewBorder(
		container.NewVBox(
			container.NewHBox(
				widget.NewLabelWithStyle("Dedup Runs", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
			),
			widget.NewSeparator(),
		),
		nil, nil, nil,
		container.NewVScroll(runsList),
	))

	right := Card(container.NewBorder(
		container.NewVBox(
			widget.NewLabelWithStyle("Match Candidates", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
			widget.NewSeparator(),
		),
		decisionCard,
		nil, nil,
		container.NewVScroll(matchesList),
	))

	matchSplit := container.NewHSplit(left, right)
	matchSplit.Offset = 0.38

	refreshRuns()

	return container.NewVScroll(container.NewVBox(
		controlsCard,
		matchSplit,
	))
}

func formatDedupRunItem(item model.DedupRun) string {
	return fmt.Sprintf("%s | %s | candidates=%d matches=%d", item.RunID, item.Status, item.TotalCandidates, item.TotalMatches)
}

func formatDedupMatchItem(item model.DedupMatch) string {
	return fmt.Sprintf("%s | score=%.2f | %s", item.MatchID, item.TotalScore, item.DecisionStatus)
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
