package ui

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"dedup/internal/config"
	"dedup/internal/dedup"
	"dedup/internal/exporter"
	"dedup/internal/importer"
	"dedup/internal/jobs"
	"dedup/internal/psgc"
	"dedup/internal/repository"
	"dedup/internal/service"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/data/binding"
)

// Dependencies collects the services and state used by the desktop UI.
type Dependencies struct {
	Config             config.Config
	DBPath             string
	PSGCReport         *psgc.Report
	Repository         *repository.Repository
	BeneficiaryService *service.BeneficiaryService
	Importer           *importer.Importer
	Exporter           *exporter.Exporter
	DedupEngine        *dedup.Engine
	DedupDecision      *service.DedupDecisionService
	BackupService      *service.BackupService
	JobManager         *jobs.Manager

	StatusMessage binding.String
	Busy          binding.Bool
	Activity      binding.String
}

// NewDependencies builds the UI runtime inputs from the bootstrap result.
func NewDependencies(
	cfg config.Config,
	dbPath string,
	psgcReport *psgc.Report,
	repo *repository.Repository,
	beneficiaryService *service.BeneficiaryService,
	importerSvc *importer.Importer,
	exporterSvc *exporter.Exporter,
	dedupEngine *dedup.Engine,
	dedupDecision *service.DedupDecisionService,
	backupSvc *service.BackupService,
	jobManager *jobs.Manager,
) (*Dependencies, error) {
	if strings.TrimSpace(cfg.AppID) == "" {
		return nil, fmt.Errorf("app id is required")
	}
	if strings.TrimSpace(cfg.WindowTitle) == "" {
		return nil, fmt.Errorf("window title is required")
	}
	if strings.TrimSpace(dbPath) == "" {
		return nil, fmt.Errorf("db path is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if beneficiaryService == nil {
		return nil, fmt.Errorf("beneficiary service is required")
	}
	if importerSvc == nil {
		return nil, fmt.Errorf("importer is required")
	}
	if exporterSvc == nil {
		return nil, fmt.Errorf("exporter is required")
	}
	if dedupEngine == nil {
		return nil, fmt.Errorf("dedup engine is required")
	}
	if dedupDecision == nil {
		return nil, fmt.Errorf("dedup decision service is required")
	}
	if backupSvc == nil {
		return nil, fmt.Errorf("backup service is required")
	}
	if jobManager == nil {
		return nil, fmt.Errorf("job manager is required")
	}

	return &Dependencies{
		Config:             cfg,
		DBPath:             dbPath,
		PSGCReport:         psgcReport,
		Repository:         repo,
		BeneficiaryService: beneficiaryService,
		Importer:           importerSvc,
		Exporter:           exporterSvc,
		DedupEngine:        dedupEngine,
		DedupDecision:      dedupDecision,
		BackupService:      backupSvc,
		JobManager:         jobManager,
		StatusMessage:      binding.NewString(),
		Busy:               binding.NewBool(),
		Activity:           binding.NewString(),
	}, nil
}

// Runtime adds the live window handle to the shared dependencies.
type Runtime struct {
	*Dependencies
	Window fyne.Window
}

func (r *Runtime) validateReady() error {
	if r == nil {
		return fmt.Errorf("runtime is nil")
	}
	if r.Dependencies == nil {
		return fmt.Errorf("dependencies are nil")
	}
	if r.Window == nil {
		return fmt.Errorf("window is nil")
	}
	return nil
}

// SetStatus updates the shared footer message on the UI thread.
func (r *Runtime) SetStatus(message string) {
	if r == nil || r.StatusMessage == nil {
		return
	}
	fyne.Do(func() {
		_ = r.StatusMessage.Set(strings.TrimSpace(message))
	})
}

// SetActivity updates the shared status detail on the UI thread.
func (r *Runtime) SetActivity(message string) {
	if r == nil || r.Activity == nil {
		return
	}
	fyne.Do(func() {
		_ = r.Activity.Set(strings.TrimSpace(message))
	})
}

// SetBusy toggles the shared background activity flag on the UI thread.
func (r *Runtime) SetBusy(busy bool) {
	if r == nil || r.Busy == nil {
		return
	}
	fyne.Do(func() {
		_ = r.Busy.Set(busy)
	})
}

// RunAsync executes work off the UI thread and reports completion safely.
func (r *Runtime) RunAsync(message string, fn func() error) {
	if r == nil {
		return
	}
	r.SetBusy(true)
	r.SetStatus(message)

	go func() {
		err := fn()
		fyne.Do(func() {
			r.SetBusy(false)
			if err != nil {
				_ = r.StatusMessage.Set("Error: " + err.Error())
				_ = r.Activity.Set(err.Error())
				return
			}
			if trimmed := strings.TrimSpace(message); trimmed != "" {
				_ = r.StatusMessage.Set(trimmed)
			}
		})
	}()
}

type screenFactory struct {
	Order int
	Name  string
	Build func(*Runtime) fyne.CanvasObject
}

var (
	screenRegistryMu sync.RWMutex
	screenRegistry   = []screenFactory{}
)

func registerScreen(order int, name string, build func(*Runtime) fyne.CanvasObject) {
	if strings.TrimSpace(name) == "" || build == nil {
		return
	}

	screenRegistryMu.Lock()
	defer screenRegistryMu.Unlock()

	screenRegistry = append(screenRegistry, screenFactory{
		Order: order,
		Name:  strings.TrimSpace(name),
		Build: build,
	})
}

func snapshotScreenRegistry() []screenFactory {
	screenRegistryMu.RLock()
	defer screenRegistryMu.RUnlock()

	items := make([]screenFactory, len(screenRegistry))
	copy(items, screenRegistry)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Order == items[j].Order {
			return items[i].Name < items[j].Name
		}
		return items[i].Order < items[j].Order
	})
	return items
}
