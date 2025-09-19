package exporter

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/xml"
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
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
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

	mu        sync.Mutex
	cond      *sync.Cond
	running   bool
	paused    bool
	cancel    context.CancelFunc
	done      chan struct{}
	startTime time.Time

	files        []DumpFile
	queue        []DumpFile
	processed    map[string]bool
	byLangTotal  map[string]int
	byLangDone   map[string]int
	pages        int
	downloadErrs int
	readErrs     int
	metrics      metricsState
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
	}
	svc.cond = sync.NewCond(&svc.mu)
	prom.update(svc.metrics)
	return svc
}

func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		if s.paused {
			log.Printf("resuming export")
			s.paused = false
			s.metrics.Status = "running"
			s.cond.Broadcast()
			return nil
		}
		return ErrExportRunning
	}

	inv, err := s.lister.List(ctx)
	if err != nil {
		return err
	}
	files := append([]DumpFile(nil), inv.Files...)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Language == files[j].Language {
			return files[i].Name < files[j].Name
		}
		return files[i].Language < files[j].Language
	})

	processed, err := s.status.Load()
	if err != nil {
		return err
	}

	s.files = files
	s.queue = make([]DumpFile, 0, len(files))
	s.processed = processed
	s.byLangTotal = make(map[string]int)
	s.byLangDone = make(map[string]int)
	langs := map[string]struct{}{}
	for _, f := range files {
		s.byLangTotal[f.Language]++
		if !processed[f.Name] {
			s.queue = append(s.queue, f)
		} else {
			s.byLangDone[f.Language]++
		}
		langs[f.Language] = struct{}{}
	}
	processedLangs := 0
	for name, total := range s.byLangTotal {
		if total == 0 {
			continue
		}
		if s.byLangDone[name] == total {
			processedLangs++
		}
	}
	total := len(s.queue)
	s.pages = 0
	s.downloadErrs = 0
	s.readErrs = 0
	s.metrics = metricsState{
		Status:             "running",
		TotalFiles:         len(files),
		ProcessedFiles:     len(files) - len(s.queue),
		TotalLanguages:     len(langs),
		ProcessedLanguages: processedLangs,
	}
	s.metrics.Progress = s.progress()
	s.startTime = time.Now()
	s.running = true
	s.paused = false
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})

	go s.run(ctx)
	log.Printf("export starting with %d pending files", total)
	return nil
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
	processed := make([]string, 0, len(s.files))
	pending := make([]string, 0, len(s.queue))
	for _, f := range s.files {
		if s.processed != nil && s.processed[f.Name] {
			processed = append(processed, f.URL)
		}
	}
	for _, f := range s.queue {
		pending = append(pending, f.URL)
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
	m.Progress = s.progress()
	m.ProcessedPages = s.pages
	m.DownloadErrors = s.downloadErrs
	m.ReadErrors = s.readErrs
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
		if ctx.Err() != nil {
			s.metrics.Status = "aborted"
		} else if len(s.queue) == 0 {
			s.metrics.Status = "completed"
		} else {
			s.metrics.Status = "idle"
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
			s.downloadErrs++
			s.metrics.DownloadErrors = s.downloadErrs
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
		err := s.processDump(ctx, dest, file)
		if err == nil {
			break
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		s.mu.Lock()
		s.readErrs++
		s.metrics.ReadErrors = s.readErrs
		s.mu.Unlock()
		log.Printf("processing error for %s: %v", file.Name, err)
		if !sleepWithContext(ctx, readRetry) {
			return context.Canceled
		}
	}

	if err := s.status.Append(file.Name); err != nil {
		log.Printf("status file append failed: %v", err)
	}

	s.mu.Lock()
	if s.processed == nil {
		s.processed = make(map[string]bool)
	}
	if !s.processed[file.Name] {
		s.processed[file.Name] = true
		s.metrics.ProcessedFiles++
		s.metrics.Progress = s.progress()
		s.byLangDone[file.Language]++
		if s.byLangDone[file.Language] == s.byLangTotal[file.Language] {
			s.metrics.ProcessedLanguages++
		}
	}
	s.mu.Unlock()
	log.Printf("finished %s", file.Name)
	return nil
}

func (s *Service) progress() float64 {
	if s.metrics.TotalFiles == 0 {
		return 0
	}
	return float64(s.metrics.ProcessedFiles) / float64(s.metrics.TotalFiles)
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
	reader, err := openDump(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	decoder := xml.NewDecoder(bufio.NewReader(reader))
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "page" {
			continue
		}
		var page xmlPage
		if err := decoder.DecodeElement(&page, &start); err != nil {
			return err
		}
		title := strings.TrimSpace(page.Title)
		if title == "" {
			continue
		}
		payload := &pageproto.Page{
			SrcUrl:   buildPageURL(file.Language, title),
			HttpCode: 200,
			Content:  []byte(page.Revision.Text.Value),
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
			if err := s.sink.Send(ctx, []byte(title), data); err != nil {
				log.Printf("sink error for %s: %v", title, err)
				if !sleepWithContext(ctx, sendRetry) {
					return context.Canceled
				}
				continue
			}
			break
		}
		s.mu.Lock()
		s.pages++
		s.metrics.ProcessedPages = s.pages
		s.mu.Unlock()
	}
}

type xmlPage struct {
	Title    string      `xml:"title"`
	Revision xmlRevision `xml:"revision"`
}

type xmlRevision struct {
	Text xmlText `xml:"text"`
}

type xmlText struct {
	Value string `xml:",chardata"`
}

func openDump(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasSuffix(path, ".bz2"):
		return &bzReader{Reader: bzip2.NewReader(file), Closer: file}, nil
	case strings.HasSuffix(path, ".gz"):
		gz, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, err
		}
		return gz, nil
	default:
		return file, nil
	}
}

type bzReader struct {
	io.Reader
	Closer io.Closer
}

func (b *bzReader) Close() error { return b.Closer.Close() }

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
