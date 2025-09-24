package exporter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	zim "github.com/akhenakh/gozim"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/example/wikipedia-parser/internal/config"
	"github.com/example/wikipedia-parser/internal/pageproto"
)

const (
	downloadRetry = 5 * time.Second
	sendRetry     = 5 * time.Second
	readRetry     = 5 * time.Second
	chromeUA      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

var wikipediaAddr = func() uint32 {
	ip := net.ParseIP("185.15.59.224").To4()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip)
}()

var (
	ErrExportRunning    = errors.New("export already running")
	ErrExportNotRunning = errors.New("export not running")
)

type Sink interface {
	Send(ctx context.Context, key, value []byte) error
}

type DumpFile struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Language string `json:"language"`
}

type Inventory struct {
	Languages []string   `json:"languages"`
	Files     []DumpFile `json:"files"`
	Errors    []string   `json:"errors"`
}

type DumpLister interface {
	List(ctx context.Context) (Inventory, error)
}

type ListResult struct {
	Processed []string `json:"processed"`
	Pending   []string `json:"pending"`
}

type ListAllResult struct {
	Files  []DumpFile `json:"files"`
	Errors []string   `json:"errors"`
}

type exportState struct {
	Files     map[string]DumpFile `json:"files"`
	Pending   []string            `json:"pending"`
	Completed []string            `json:"completed"`
}

type metricsState struct {
	Status             string
	Progress           float64
	Elapsed            time.Duration
	ETA                time.Duration
	ProcessedFiles     int
	TotalFiles         int
	ProcessedLanguages int
	TotalLanguages     int
	ProcessedPages     int
	DownloadErrors     int
	ReadErrors         int
}

type promMetrics struct {
	handler        http.Handler
	progress       prometheus.Gauge
	elapsed        prometheus.Gauge
	eta            prometheus.Gauge
	processedFiles prometheus.Gauge
	totalFiles     prometheus.Gauge
	processedLangs prometheus.Gauge
	totalLangs     prometheus.Gauge
	processedPages prometheus.Gauge
	downloadErrors prometheus.Gauge
	readErrors     prometheus.Gauge
	status         *prometheus.GaugeVec
	states         []string
}

func newPromMetrics() *promMetrics {
	progress := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_progress",
		Help: "Overall progress",
	})
	elapsed := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_elapsed_seconds",
		Help: "Elapsed time in seconds",
	})
	eta := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_eta_seconds",
		Help: "Estimated time to finish in seconds",
	})
	processedFiles := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_processed_files",
		Help: "Processed files",
	})
	totalFiles := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_total_files",
		Help: "Total files",
	})
	processedLangs := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_processed_languages",
		Help: "Processed languages",
	})
	totalLangs := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_total_languages",
		Help: "Total languages",
	})
	processedPages := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_processed_pages",
		Help: "Processed pages",
	})
	downloadErrors := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_download_errors",
		Help: "Download errors",
	})
	readErrors := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_read_errors",
		Help: "Read errors",
	})
	status := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "exporter_status",
		Help: "Exporter status",
	}, []string{"state"})

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		progress,
		elapsed,
		eta,
		processedFiles,
		totalFiles,
		processedLangs,
		totalLangs,
		processedPages,
		downloadErrors,
		readErrors,
		status,
	)

	states := []string{"idle", "running", "paused", "completed", "aborted"}
	for _, st := range states {
		status.WithLabelValues(st).Set(0)
	}
	status.WithLabelValues("idle").Set(1)

	return &promMetrics{
		handler:        promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		progress:       progress,
		elapsed:        elapsed,
		eta:            eta,
		processedFiles: processedFiles,
		totalFiles:     totalFiles,
		processedLangs: processedLangs,
		totalLangs:     totalLangs,
		processedPages: processedPages,
		downloadErrors: downloadErrors,
		readErrors:     readErrors,
		status:         status,
		states:         states,
	}
}

func (m *promMetrics) update(state metricsState) {
	m.progress.Set(state.Progress)
	m.elapsed.Set(state.Elapsed.Seconds())
	m.eta.Set(state.ETA.Seconds())
	m.processedFiles.Set(float64(state.ProcessedFiles))
	m.totalFiles.Set(float64(state.TotalFiles))
	m.processedLangs.Set(float64(state.ProcessedLanguages))
	m.totalLangs.Set(float64(state.TotalLanguages))
	m.processedPages.Set(float64(state.ProcessedPages))
	m.downloadErrors.Set(float64(state.DownloadErrors))
	m.readErrors.Set(float64(state.ReadErrors))
	for _, st := range m.states {
		value := 0.0
		if state.Status == st {
			value = 1
		}
		m.status.WithLabelValues(st).Set(value)
	}
}

type Service struct {
	cfg    config.Config
	client *http.Client
	sink   Sink
	lister DumpLister
	status *statusFile
	prom   *promMetrics

	openZim zimOpener

	mu        sync.Mutex
	cond      *sync.Cond
	running   bool
	paused    bool
	cancel    context.CancelFunc
	done      chan struct{}
	startTime time.Time

	state   exportState
	queue   []DumpFile
	metrics metricsState
}

func NewService(cfg config.Config, client *http.Client, sink Sink) *Service {
	return NewServiceWithLister(cfg, client, sink, NewHTTPDumpLister(cfg.DumpBaseURL, client))
}

func NewServiceWithLister(cfg config.Config, client *http.Client, sink Sink, lister DumpLister) *Service {
	prom := newPromMetrics()
	svc := &Service{
		cfg:     cfg,
		client:  client,
		sink:    sink,
		lister:  lister,
		status:  newStatusFile(cfg.StatusFile),
		metrics: metricsState{Status: "idle"},
		prom:    prom,
		openZim: openZimArchive,
	}
	svc.cond = sync.NewCond(&svc.mu)
	prom.update(svc.metrics)
	return svc
}

func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		if s.paused {
			log.Printf("resuming export")
			s.paused = false
			s.metrics.Status = "running"
			s.cond.Broadcast()
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
		return ErrExportRunning
	}
	s.mu.Unlock()

	state, queue, err := s.prepareJob(ctx)
	if err != nil {
		return err
	}
	pending := len(queue)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return ErrExportRunning
	}
	s.state = state
	s.queue = queue
	totalLangs, processedLangs := languageStats(state)
	s.metrics = metricsState{
		Status:             "running",
		TotalFiles:         len(state.Files),
		ProcessedFiles:     len(state.Completed),
		TotalLanguages:     totalLangs,
		ProcessedLanguages: processedLangs,
		Progress:           s.progressLocked(),
		DownloadErrors:     0,
		ReadErrors:         0,
		ProcessedPages:     0,
	}
	s.startTime = time.Now()
	s.running = true
	s.paused = false
	runCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})

	go s.run(runCtx)
	log.Printf("export starting with %d pending files", pending)
	return nil
}

func (s *Service) prepareJob(ctx context.Context) (exportState, []DumpFile, error) {
	state, err := s.status.Load()
	if err != nil {
		return exportState{}, nil, err
	}
	if len(state.Pending) == 0 {
		state, err = s.buildFreshState(ctx)
		if err != nil {
			return exportState{}, nil, err
		}
	}
	queue := stateQueue(state)
	if len(queue) == 0 && len(state.Pending) > 0 {
		state, err = s.buildFreshState(ctx)
		if err != nil {
			return exportState{}, nil, err
		}
		queue = stateQueue(state)
	}
	return state, queue, nil
}

func (s *Service) buildFreshState(ctx context.Context) (exportState, error) {
	inv, err := s.lister.List(ctx)
	if err != nil {
		return exportState{}, err
	}
	files := append([]DumpFile(nil), inv.Files...)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Language == files[j].Language {
			return files[i].Name < files[j].Name
		}
		return files[i].Language < files[j].Language
	})
	state := exportState{
		Files:   make(map[string]DumpFile, len(files)),
		Pending: make([]string, 0, len(files)),
	}
	for _, file := range files {
		state.Files[file.Name] = file
		state.Pending = append(state.Pending, file.Name)
	}
	if err := s.status.Save(state); err != nil {
		return exportState{}, err
	}
	return state, nil
}

func (s *Service) Pause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return ErrExportNotRunning
	}
	if s.paused {
		return nil
	}
	log.Printf("pausing export")
	s.paused = true
	s.metrics.Status = "paused"
	return nil
}

func (s *Service) Abort() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrExportNotRunning
	}
	log.Printf("aborting export")
	cancel := s.cancel
	done := s.done
	s.paused = false
	s.cond.Broadcast()
	s.mu.Unlock()

	cancel()
	<-done
	return nil
}

func (s *Service) List() ListResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	processed := make([]string, 0, len(s.state.Completed))
	for _, name := range s.state.Completed {
		if file, ok := s.state.Files[name]; ok {
			processed = append(processed, file.URL)
		}
	}
	pending := make([]string, 0, len(s.state.Pending))
	for _, name := range s.state.Pending {
		if file, ok := s.state.Files[name]; ok {
			pending = append(pending, file.URL)
		}
	}
	return ListResult{Processed: processed, Pending: pending}
}

func (s *Service) ListAll(ctx context.Context) (ListAllResult, error) {
	inv, err := s.lister.List(ctx)
	if err != nil {
		return ListAllResult{Errors: []string{err.Error()}}, err
	}
	return ListAllResult{Files: inv.Files, Errors: inv.Errors}, nil
}

func (s *Service) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshot := s.snapshotMetrics()
		s.prom.update(snapshot)
		s.prom.handler.ServeHTTP(w, r)
	})
}

func (s *Service) snapshotMetrics() metricsState {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := s.metrics
	m.Progress = s.progressLocked()
	if s.running {
		elapsed := time.Since(s.startTime)
		m.Elapsed = elapsed
		if m.ProcessedFiles > 0 && m.TotalFiles > 0 {
			perFile := elapsed / time.Duration(m.ProcessedFiles)
			remaining := m.TotalFiles - m.ProcessedFiles
			if remaining > 0 {
				m.ETA = time.Duration(int64(perFile) * int64(remaining))
			} else {
				m.ETA = 0
			}
		} else {
			m.ETA = 0
		}
	} else {
		m.Elapsed = 0
		m.ETA = 0
	}
	s.metrics = m
	return m
}

func (s *Service) Wait() {
	s.mu.Lock()
	done := s.done
	running := s.running
	s.mu.Unlock()
	if !running {
		return
	}
	<-done
}

func (s *Service) run(ctx context.Context) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.paused = false
		s.metrics.Progress = s.progressLocked()
		if ctx.Err() != nil {
			s.metrics.Status = "aborted"
		} else if s.metrics.Status != "aborted" {
			if len(s.state.Pending) == 0 {
				s.metrics.Status = "completed"
			} else {
				s.metrics.Status = "idle"
			}
		}
		close(s.done)
		s.mu.Unlock()
	}()

	for len(s.queue) > 0 {
		s.mu.Lock()
		for s.paused {
			s.cond.Wait()
		}
		if ctx.Err() != nil {
			s.mu.Unlock()
			return
		}
		file := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()

		if err := s.handleFile(ctx, file); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("fatal error handling %s: %v", file.Name, err)
			s.mu.Lock()
			s.metrics.Status = "aborted"
			s.mu.Unlock()
			return
		}
	}
}

func (s *Service) handleFile(ctx context.Context, file DumpFile) error {
	dest := filepath.Join(s.cfg.WorkDir, file.Name)
	defer os.Remove(dest)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("downloading %s", file.URL)
		if err := s.download(ctx, file.URL, dest); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			s.mu.Lock()
			s.metrics.DownloadErrors++
			s.mu.Unlock()
			log.Printf("download error for %s: %v", file.URL, err)
			if !sleepWithContext(ctx, downloadRetry) {
				return context.Canceled
			}
			continue
		}
		break
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("processing %s", file.Name)
		if err := s.processDump(ctx, dest, file); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			s.mu.Lock()
			s.metrics.ReadErrors++
			s.mu.Unlock()
			log.Printf("processing error for %s: %v", file.Name, err)
			if !sleepWithContext(ctx, readRetry) {
				return context.Canceled
			}
			continue
		}
		break
	}

	if err := s.markFileDone(file); err != nil {
		log.Printf("status update failed for %s: %v", file.Name, err)
	}
	log.Printf("finished %s", file.Name)
	return nil
}

func (s *Service) markFileDone(file DumpFile) error {
	s.mu.Lock()
	if containsName(s.state.Completed, file.Name) {
		s.mu.Unlock()
		return nil
	}
	s.state.Pending = removeName(s.state.Pending, file.Name)
	s.state.Completed = append(s.state.Completed, file.Name)
	s.metrics.ProcessedFiles = len(s.state.Completed)
	totalLangs, processedLangs := languageStats(s.state)
	s.metrics.TotalLanguages = totalLangs
	s.metrics.ProcessedLanguages = processedLangs
	s.metrics.Progress = s.progressLocked()
	stateCopy := s.state
	s.mu.Unlock()
	return s.status.Save(stateCopy)
}

func (s *Service) download(ctx context.Context, src, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer out.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", chromeUA)
	resp, err := s.client.Do(req)
	if err != nil {
		os.Remove(tmp)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Remove(tmp)
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dest)
}

func (s *Service) processDump(ctx context.Context, path string, file DumpFile) error {
	archive, err := s.openZim(path)
	if err != nil {
		return err
	}
	defer archive.Close()

	return processZimArchive(ctx, archive, file, s.sink, &s.mu, &s.metrics)
}

func processZimArchive(ctx context.Context, archive zimArchive, file DumpFile, sink Sink, mu *sync.Mutex, metrics *metricsState) error {
	return archive.Walk(ctx, func(entry zimEntry) error {
		if !isArticleEntry(entry) {
			return nil
		}
		title := strings.TrimSpace(entry.Title())
		if title == "" {
			return nil
		}
		key := articleKey(entry)
		if key == "" {
			return nil
		}
		content, err := entry.Data()
		if err != nil {
			return err
		}
		if len(content) == 0 {
			return nil
		}
		payload := &pageproto.Page{
			SrcUrl:   buildPageURL(file.Language, title),
			HttpCode: 200,
			Content:  content,
			Ip:       wikipediaIP(),
		}
		data, err := pageproto.Marshal(payload)
		if err != nil {
			return err
		}
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := sink.Send(ctx, []byte(key), data); err != nil {
				log.Printf("sink error for %s: %v", key, err)
				if !sleepWithContext(ctx, sendRetry) {
					return context.Canceled
				}
				continue
			}
			break
		}
		if mu != nil && metrics != nil {
			mu.Lock()
			metrics.ProcessedPages++
			mu.Unlock()
		}
		return nil
	})
}

func isArticleEntry(entry zimEntry) bool {
	if entry == nil {
		return false
	}
	if entry.Namespace() != 'A' {
		return false
	}
	mime := strings.ToLower(entry.MimeType())
	if mime == "" {
		return false
	}
	if !strings.HasPrefix(mime, "text/html") && !strings.HasPrefix(mime, "application/xhtml") {
		return false
	}
	title := strings.TrimSpace(entry.Title())
	if title == "" {
		return false
	}
	slug := strings.ToLower(articleSlug(entry))
	if slug == "" {
		slug = strings.ToLower(strings.ReplaceAll(title, " ", "_"))
	}
	slug = strings.Trim(slug, "/")
	if slug == "" {
		return false
	}
	base := slug
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	base = strings.TrimSpace(base)
	if base == "" {
		return false
	}
	switch base {
	case "readme", "metadata", "meta", "description", "favicon", "license", "copyright":
		return false
	}
	return true
}

func articleKey(entry zimEntry) string {
	slug := articleSlug(entry)
	if slug == "" {
		slug = entry.Title()
	}
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return ""
	}
	if idx := strings.LastIndex(slug, "/"); idx >= 0 {
		slug = slug[idx+1:]
	}
	slug = strings.ReplaceAll(slug, " ", "_")
	slug = strings.ReplaceAll(slug, "/", "_")
	if slug == "" {
		return ""
	}
	return slug
}

func articleSlug(entry zimEntry) string {
	url := entry.URL()
	if url == "" {
		return ""
	}
	if idx := strings.Index(url, "/"); idx >= 0 {
		url = url[idx+1:]
	}
	if idx := strings.LastIndex(url, "/"); idx >= 0 {
		url = url[idx+1:]
	}
	url = strings.TrimSpace(url)
	if url == "" {
		return ""
	}
	if strings.HasPrefix(url, "wiki/") {
		url = url[len("wiki/"):]
	}
	if idx := strings.Index(url, "?"); idx >= 0 {
		url = url[:idx]
	}
	if idx := strings.Index(url, "#"); idx >= 0 {
		url = url[:idx]
	}
	if idx := strings.LastIndex(url, "."); idx >= 0 {
		ext := strings.ToLower(url[idx+1:])
		if ext == "html" || ext == "htm" {
			url = url[:idx]
		}
	}
	return url
}

type zimOpener func(path string) (zimArchive, error)

type zimArchive interface {
	Close() error
	Walk(ctx context.Context, fn func(zimEntry) error) error
}

type zimEntry interface {
	Namespace() byte
	Title() string
	URL() string
	MimeType() string
	Data() ([]byte, error)
}

func openZimArchive(path string) (zimArchive, error) {
	reader, err := zim.NewReader(path, false)
	if err != nil {
		return nil, err
	}
	return &goZimArchive{reader: reader}, nil
}

type goZimArchive struct {
	reader *zim.ZimReader
}

func (a *goZimArchive) Close() error {
	if a.reader == nil {
		return nil
	}
	return a.reader.Close()
}

func (a *goZimArchive) Walk(ctx context.Context, fn func(zimEntry) error) error {
	if a.reader == nil {
		return nil
	}
	for idx := uint32(1); idx < a.reader.ArticleCount; idx++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		article, err := a.reader.ArticleAtURLIdx(idx)
		if err != nil {
			continue
		}
		if article == nil {
			continue
		}
		if err := fn(goZimEntry{article: article}); err != nil {
			return err
		}
	}
	return nil
}

type goZimEntry struct {
	article *zim.Article
}

func (e goZimEntry) Namespace() byte {
	if e.article == nil {
		return 0
	}
	return e.article.Namespace
}

func (e goZimEntry) Title() string {
	if e.article == nil {
		return ""
	}
	return strings.TrimSpace(e.article.Title)
}

func (e goZimEntry) URL() string {
	if e.article == nil {
		return ""
	}
	return e.article.FullURL()
}

func (e goZimEntry) MimeType() string {
	if e.article == nil {
		return ""
	}
	return e.article.MimeType()
}

func (e goZimEntry) Data() ([]byte, error) {
	if e.article == nil {
		return nil, nil
	}
	return e.article.Data()
}

func ConvertFile(ctx context.Context, cfg config.Config, sink Sink, path string) error {
	archive, err := openZimArchive(path)
	if err != nil {
		return err
	}
	defer archive.Close()

	name := filepath.Base(path)
	lang := datasetLanguage(strings.TrimSuffix(name, ".zim"))
	if lang == "" {
		return fmt.Errorf("cannot determine language from %s", name)
	}
	file := DumpFile{Name: name, Language: lang}
	return processZimArchive(ctx, archive, file, sink, nil, nil)
}

func buildPageURL(lang, title string) string {
	slug := strings.ReplaceAll(title, " ", "_")
	escaped := url.PathEscape(slug)
	return fmt.Sprintf("https://%s.wikipedia.org/wiki/%s", lang, escaped)
}

func wikipediaIP() uint32 { return wikipediaAddr }

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func countProcessedLanguages(total, done map[string]int) int {
	processed := 0
	for lang, totalCount := range total {
		if totalCount == 0 {
			continue
		}
		if done[lang] >= totalCount {
			processed++
		}
	}
	return processed
}

func (s *Service) progressLocked() float64 {
	total := len(s.state.Files)
	if total == 0 {
		return 0
	}
	completed := len(s.state.Completed)
	if completed > total {
		completed = total
	}
	return float64(completed) / float64(total)
}

func stateQueue(state exportState) []DumpFile {
	queue := make([]DumpFile, 0, len(state.Pending))
	for _, name := range state.Pending {
		if file, ok := state.Files[name]; ok {
			queue = append(queue, file)
		}
	}
	return queue
}

func languageStats(state exportState) (total, processed int) {
	totals := make(map[string]int)
	done := make(map[string]int)
	for _, file := range state.Files {
		totals[file.Language]++
	}
	for _, name := range state.Completed {
		if file, ok := state.Files[name]; ok {
			done[file.Language]++
		}
	}
	total = len(totals)
	processed = countProcessedLanguages(totals, done)
	return
}

func removeName(list []string, name string) []string {
	for i, v := range list {
		if v == name {
			copy(list[i:], list[i+1:])
			return list[:len(list)-1]
		}
	}
	return list
}

func containsName(list []string, name string) bool {
	for _, v := range list {
		if v == name {
			return true
		}
	}
	return false
}
