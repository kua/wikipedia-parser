package exporter

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
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

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	xz "github.com/xi2/xz"

	"github.com/example/wikipedia-parser/internal/config"
	"github.com/example/wikipedia-parser/internal/pageproto"
	libzim "github.com/example/wikipedia-parser/internal/zim"
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
	Dataset  string `json:"dataset,omitempty"`
	Version  string `json:"version,omitempty"`
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
	Running  bool
	Progress float64
}

type promMetrics struct {
	handler  http.Handler
	progress prometheus.Gauge
	running  prometheus.Gauge
}

func newPromMetrics() *promMetrics {
	progress := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_progress",
		Help: "Overall progress",
	})
	running := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "exporter_running",
		Help: "Exporter running state (1 running, 0 idle)",
	})

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		progress,
		running,
	)

	return &promMetrics{
		handler:  promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		progress: progress,
		running:  running,
	}
}

func (m *promMetrics) update(state metricsState) {
	m.progress.Set(state.Progress)
	if state.Running {
		m.running.Set(1)
	} else {
		m.running.Set(0)
	}
}

type Service struct {
	cfg       config.Config
	client    *http.Client
	sink      Sink
	listDumps func(ctx context.Context) ([]DumpFile, error)
	status    *statusFile
	prom      *promMetrics

	openZim zimOpener

	mu      sync.Mutex
	cond    *sync.Cond
	running bool
	cancel  context.CancelFunc

	state   exportState
	metrics metricsState
}

func NewService(cfg config.Config, client *http.Client, sink Sink) *Service {
	return NewServiceWithLister(cfg, client, sink, NewHTTPDumpLister(cfg.DumpBaseURL, client))
}

func NewServiceWithLister(cfg config.Config, client *http.Client, sink Sink, listFunc func(ctx context.Context) ([]DumpFile, error)) *Service {
	prom := newPromMetrics()
	svc := &Service{
		cfg:       cfg,
		client:    client,
		sink:      sink,
		listDumps: listFunc,
		status:    newStatusFile(cfg.StatusFile),
		metrics:   metricsState{},
		prom:      prom,
		openZim:   openZimArchive,
	}
	svc.cond = sync.NewCond(&svc.mu)
	prom.update(svc.metrics)
	return svc
}

func (s *Service) Start(ctx context.Context) error {
	state, err := s.prepareState(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return ErrExportRunning
	}
	s.state = state
	s.metrics.Progress = s.progressLocked()
	s.metrics.Running = true
	s.running = true
	runCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	go s.run(runCtx)
	log.Printf("export starting with %d pending files", len(state.Pending))
	return nil
}

func (s *Service) prepareState(ctx context.Context) (exportState, error) {
	state, err := s.status.Load()
	if err != nil {
		return exportState{}, err
	}
	if len(state.Pending) == 0 {
		state, err = s.buildFreshState(ctx)
		if err != nil {
			return exportState{}, err
		}
	}
	return state, nil
}

func (s *Service) buildFreshState(ctx context.Context) (exportState, error) {
	files, err := s.listDumps(ctx)
	if err != nil {
		return exportState{}, err
	}
	files = append([]DumpFile(nil), files...)
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
	if !s.running {
		s.mu.Unlock()
		return ErrExportNotRunning
	}
	cancel := s.cancel
	s.mu.Unlock()

	log.Printf("pausing export")
	cancel()

	s.mu.Lock()
	for s.running {
		s.cond.Wait()
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) Abort() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrExportNotRunning
	}
	cancel := s.cancel
	s.mu.Unlock()

	log.Printf("aborting export")
	cancel()

	s.mu.Lock()
	for s.running {
		s.cond.Wait()
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) List() exportState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneState(s.state)
}

func (s *Service) ListAll(ctx context.Context) (ListAllResult, error) {
	files, err := s.listDumps(ctx)
	if err != nil {
		return ListAllResult{Errors: []string{err.Error()}}, err
	}
	return ListAllResult{Files: files}, nil
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
	s.metrics = m
	return m
}

func (s *Service) run(ctx context.Context) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.metrics.Running = false
		s.metrics.Progress = s.progressLocked()
		s.cancel = nil
		s.cond.Broadcast()
		s.mu.Unlock()
	}()

	for {
		file, ok := s.nextPending()
		if !ok {
			return
		}
		if err := s.handleFile(ctx, file); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("fatal error handling %s: %v", file.Name, err)
			return
		}
	}
}

func (s *Service) nextPending() (DumpFile, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.state.Pending) > 0 {
		name := s.state.Pending[0]
		if file, ok := s.state.Files[name]; ok {
			return file, true
		}
		s.state.Pending = s.state.Pending[1:]
	}
	return DumpFile{}, false
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
	s.metrics.Progress = s.progressLocked()
	stateCopy := cloneState(s.state)
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

	return processZimArchive(ctx, archive, file, s.sink)
}

func processZimArchive(ctx context.Context, archive zimArchive, file DumpFile, sink Sink) error {
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
		content, err = normalizeHTMLContent(content)
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(content)) == 0 {
			return nil
		}
		if !isProbablyHTML(content) {
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
		return nil
	})
}

func isArticleEntry(entry zimEntry) bool {
	if entry == nil {
		return false
	}
	if entry.IsRedirect() {
		return false
	}
	if !entry.HasItem() {
		return false
	}
	mime := strings.ToLower(strings.TrimSpace(entry.MimeType()))
	if mime == "" {
		return false
	}
	if !strings.Contains(mime, "html") {
		return false
	}
	title := strings.TrimSpace(entry.Title())
	if title == "" {
		return false
	}
	slug := strings.ToLower(strings.TrimSpace(articleSlug(entry)))
	slug = strings.Trim(slug, "/")
	base := slug
	if base == "" {
		base = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(title, " ", "_")))
	}
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

const maxContentDecodePasses = 3

func normalizeHTMLContent(data []byte) ([]byte, error) {
	var err error
	for pass := 0; pass < maxContentDecodePasses; pass++ {
		if isProbablyHTML(data) {
			return data, nil
		}
		switch detectCompression(data) {
		case "gzip":
			var reader *gzip.Reader
			reader, err = gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			data, err = io.ReadAll(reader)
			closeErr := reader.Close()
			if err == nil && closeErr != nil {
				err = closeErr
			}
		case "xz":
			var xzr *xz.Reader
			xzr, err = xz.NewReader(bytes.NewReader(data), 0)
			if err != nil {
				return nil, err
			}
			data, err = io.ReadAll(xzr)
		case "zstd":
			var dec *zstd.Decoder
			dec, err = zstd.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			data, err = io.ReadAll(dec)
			dec.Close()
		case "bzip2":
			data, err = io.ReadAll(bzip2.NewReader(bytes.NewReader(data)))
		default:
			return data, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func detectCompression(data []byte) string {
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		return "gzip"
	}
	if len(data) >= 6 && bytes.HasPrefix(data, []byte{0xFD, '7', 'z', 'X', 'Z', 0x00}) {
		return "xz"
	}
	if len(data) >= 4 && bytes.HasPrefix(data, []byte{0x28, 0xB5, 0x2F, 0xFD}) {
		return "zstd"
	}
	if len(data) >= 3 && bytes.HasPrefix(data, []byte{'B', 'Z', 'h'}) {
		return "bzip2"
	}
	return ""
}

func isProbablyHTML(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return false
	}
	lower := bytes.ToLower(trimmed)
	if trimmed[0] == '<' {
		return true
	}
	return bytes.HasPrefix(lower, []byte("<!doctype")) || bytes.HasPrefix(lower, []byte("<?xml"))
}

type zimOpener func(path string) (zimArchive, error)

type zimArchive interface {
	Close() error
	Walk(ctx context.Context, fn func(zimEntry) error) error
}

type zimEntry interface {
	Title() string
	URL() string
	MimeType() string
	Data() ([]byte, error)
	IsRedirect() bool
	HasItem() bool
}

func openZimArchive(path string) (zimArchive, error) {
	file, err := libzim.Open(path)
	if err != nil {
		return nil, err
	}
	return &libzimArchive{file: file}, nil
}

type libzimArchive struct {
	file *libzim.Archive
}

func (a *libzimArchive) Close() error {
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

func (a *libzimArchive) Walk(ctx context.Context, fn func(zimEntry) error) error {
	if a.file == nil {
		return nil
	}
	total := a.file.EntryCount()
	for idx := uint32(0); idx < total; idx++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entry, err := a.file.EntryAt(idx)
		if err != nil {
			continue
		}
		current := &libzimEntry{entry: entry}
		callErr := fn(current)
		entry.Close()
		if callErr != nil {
			return callErr
		}
	}
	return nil
}

type libzimEntry struct {
	entry *libzim.Entry
}

func (e *libzimEntry) Title() string {
	if e == nil || e.entry == nil {
		return ""
	}
	return strings.TrimSpace(e.entry.Title())
}

func (e *libzimEntry) URL() string {
	if e == nil || e.entry == nil {
		return ""
	}
	return strings.TrimSpace(e.entry.Path())
}

func (e *libzimEntry) MimeType() string {
	if e == nil || e.entry == nil {
		return ""
	}
	return strings.TrimSpace(e.entry.MimeType(false))
}

func (e *libzimEntry) Data() ([]byte, error) {
	if e == nil || e.entry == nil {
		return nil, nil
	}
	return e.entry.Data()
}

func (e *libzimEntry) IsRedirect() bool {
	if e == nil || e.entry == nil {
		return false
	}
	return e.entry.IsRedirect()
}

func (e *libzimEntry) HasItem() bool {
	if e == nil || e.entry == nil {
		return false
	}
	return e.entry.HasItem()
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
	dataset := strings.TrimSuffix(name, ".zim")
	version := ""
	if ds, ver, ok := splitDatasetVersion(name); ok {
		dataset = ds
		version = ver
	}
	file := DumpFile{Name: name, Language: lang, Dataset: dataset, Version: version}
	return processZimArchive(ctx, archive, file, sink)
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

func cloneState(src exportState) exportState {
	files := make(map[string]DumpFile, len(src.Files))
	for k, v := range src.Files {
		files[k] = v
	}
	return exportState{
		Files:     files,
		Pending:   append([]string(nil), src.Pending...),
		Completed: append([]string(nil), src.Completed...),
	}
}
