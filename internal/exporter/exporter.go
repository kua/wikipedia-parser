package exporter

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
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
	reader, err := openDump(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	decoder := xml.NewDecoder(bufio.NewReader(reader))
	jobs := make(chan pageJob)
	workerErr := make(chan error, 1)

	workers := s.cfg.MaxRenderInflight
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := s.renderAndSendPage(workerCtx, job.lang, job.title, job.text); err != nil {
					select {
					case workerErr <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	var decodeErr error
loop:
	for {
		if err := workerCtx.Err(); err != nil {
			decodeErr = err
			break
		}
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			decodeErr = err
			break
		}
		startElem, ok := token.(xml.StartElement)
		if !ok || startElem.Name.Local != "page" {
			continue
		}
		var page xmlPage
		if err := decoder.DecodeElement(&page, &startElem); err != nil {
			decodeErr = err
			break
		}
		title := strings.TrimSpace(page.Title)
		if title == "" {
			continue
		}
		job := pageJob{lang: file.Language, title: title, text: page.Revision.Text.Value}
		select {
		case jobs <- job:
		case <-workerCtx.Done():
			decodeErr = workerCtx.Err()
			break loop
		}
	}

	close(jobs)
	wg.Wait()

	select {
	case err := <-workerErr:
		if err != nil {
			return err
		}
	default:
	}

	if decodeErr != nil {
		return decodeErr
	}
	if err := workerCtx.Err(); err != nil {
		return err
	}
	return nil
}

type pageJob struct {
	lang  string
	title string
	text  string
}

func (s *Service) renderAndSendPage(ctx context.Context, lang, title, text string) error {
	html, err := s.renderPage(ctx, lang, title, text)
	if err != nil {
		return fmt.Errorf("render %s/%s: %w", lang, title, err)
	}
	payload := &pageproto.Page{
		SrcUrl:   buildPageURL(lang, title),
		HttpCode: 200,
		Headers: map[string]string{
			"Content-Type": "text/html; charset=utf-8",
		},
		Content: html,
		Ip:      wikipediaIP(),
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
		s.mu.Lock()
		s.metrics.ProcessedPages++
		s.mu.Unlock()
		return nil
	}
}

func (s *Service) renderPage(ctx context.Context, lang, title, text string) ([]byte, error) {
	if strings.TrimSpace(text) == "" {
		return []byte{}, nil
	}
	form := url.Values{}
	form.Set("action", "parse")
	form.Set("format", "json")
	form.Set("formatversion", "2")
	form.Set("prop", "text")
	form.Set("contentmodel", "wikitext")
	form.Set("title", title)
	form.Set("text", text)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.MediaWikiAPI, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", chromeUA)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mediawiki render status %s for %s/%s", resp.Status, lang, title)
	}

	var parsed mediaWikiParseResponse
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 512 {
			snippet = snippet[:512] + "..."
		}
		return nil, fmt.Errorf("mediawiki render decode error for %s/%s: %w (body snippet: %q)", lang, title, err, snippet)
	}
	if parsed.Error != nil {
		return nil, fmt.Errorf("mediawiki render error %s: %s", parsed.Error.Code, parsed.Error.Info)
	}
	return []byte(parsed.Parse.Text), nil
}

type mediaWikiParseResponse struct {
	Parse struct {
		Text string `json:"text"`
	} `json:"parse"`
	Error *struct {
		Code string `json:"code"`
		Info string `json:"info"`
	} `json:"error"`
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
