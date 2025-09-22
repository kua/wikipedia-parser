package exporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/example/wikipedia-parser/internal/config"
	"github.com/example/wikipedia-parser/internal/pageproto"
)

type memorySink struct {
	mu    sync.Mutex
	pages []*pageproto.Page
}

func (m *memorySink) Send(ctx context.Context, key, value []byte) error {
	page, err := pageproto.Unmarshal(value)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pages = append(m.pages, page)
	return nil
}

func TestServiceRun(t *testing.T) {
	tmp := t.TempDir()
	xml := `<mediawiki><page><title>Foo Bar</title><revision><text>hello</text></revision></page><page><title>Baz</title><revision><text>world</text></revision></page></mediawiki>`
	gzData := gzipData([]byte(xml))

	type requestLog struct {
		mu    sync.Mutex
		agent map[string]string
	}
	reqLog := &requestLog{agent: make(map[string]string)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqLog.mu.Lock()
		reqLog.agent[r.URL.Path] = r.Header.Get("User-Agent")
		reqLog.mu.Unlock()
		switch r.URL.Path {
		case "/conf/dblists/wikipedia.dblist":
			w.Write([]byte("enwiki\nruwiki\n"))
		case "/enwiki/latest/":
			w.Write([]byte(`<a href="enwiki-latest-pages-articles1.xml-p1p2.gz">chunk</a>`))
		case "/ruwiki/latest/":
			w.Write([]byte(`<a href="ruwiki-latest-pages-articles1.xml-p1p2.gz">chunk</a>`))
		case "/enwiki/latest/enwiki-latest-pages-articles1.xml-p1p2.gz":
			w.Write(gzData)
		case "/ruwiki/latest/ruwiki-latest-pages-articles1.xml-p1p2.gz":
			w.Write(gzData)
		case "/api.php":
			if err := r.ParseForm(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			text := r.FormValue("text")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"parse": map[string]string{"text": fmt.Sprintf("<p>%s</p>", text)}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := server.Client()
	cfg := config.Config{
		DumpBaseURL:       server.URL,
		WorkDir:           tmp,
		HTTPAddr:          ":0",
		Sink:              "stdout",
		StatusFile:        filepath.Join(tmp, "status.log"),
		MediaWikiAPI:      server.URL + "/api.php",
		MaxRenderInflight: 2,
	}
	lister := &httpLister{base: cfg.DumpBaseURL, client: client, dblistURL: server.URL + "/conf/dblists/wikipedia.dblist"}
	sink := &memorySink{}
	svc := NewServiceWithLister(cfg, client, sink, lister)

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	svc.Wait()

	if len(sink.pages) != 4 {
		t.Fatalf("expected 4 pages, got %d", len(sink.pages))
	}
	pagesByURL := make(map[string]*pageproto.Page)
	for _, page := range sink.pages {
		pagesByURL[page.GetSrcUrl()] = page
	}
	fooPage, ok := pagesByURL["https://en.wikipedia.org/wiki/Foo_Bar"]
	if !ok {
		t.Fatalf("foo page missing: %v", pagesByURL)
	}
	if fooPage.GetHttpCode() != 200 || fooPage.GetIp() == 0 {
		t.Fatalf("unexpected page fields: %+v", fooPage)
	}
	if ct := fooPage.GetHeaders()["Content-Type"]; ct != "text/html; charset=utf-8" {
		t.Fatalf("unexpected content type: %q", ct)
	}
	if body := string(fooPage.GetContent()); body != "<p>hello</p>" {
		t.Fatalf("unexpected html content: %s", body)
	}

	list := svc.List()
	if len(list.Pending) != 0 {
		t.Fatalf("expected no pending files, got %v", list.Pending)
	}
	if len(list.Processed) != 2 {
		t.Fatalf("expected 2 processed files, got %v", list.Processed)
	}

	data, err := os.ReadFile(cfg.StatusFile)
	if err != nil {
		t.Fatalf("status file read: %v", err)
	}
	var state exportState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("status decode: %v", err)
	}
	if len(state.Pending) != 0 {
		t.Fatalf("expected no pending items, got %v", state.Pending)
	}
	if len(state.Completed) != 2 {
		t.Fatalf("expected 2 completed items, got %v", state.Completed)
	}
	if _, ok := state.Files["enwiki-latest-pages-articles1.xml-p1p2.gz"]; !ok {
		t.Fatalf("status missing enwiki file: %+v", state.Files)
	}
	for _, name := range []string{
		"enwiki-latest-pages-articles1.xml-p1p2.gz",
		"ruwiki-latest-pages-articles1.xml-p1p2.gz",
	} {
		if _, err := os.Stat(filepath.Join(tmp, name)); err == nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dump file still present %s: %v", name, err)
		}
	}

	if agent := reqLog.agent["/conf/dblists/wikipedia.dblist"]; agent != chromeUA {
		t.Fatalf("unexpected dblist user agent: %q", agent)
	}
	if agent := reqLog.agent["/enwiki/latest/enwiki-latest-pages-articles1.xml-p1p2.gz"]; agent != chromeUA {
		t.Fatalf("unexpected download user agent: %q", agent)
	}

	metricsReq := httptest.NewRecorder()
	svc.MetricsHandler().ServeHTTP(metricsReq, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metricsReq.Code != http.StatusOK {
		t.Fatalf("metrics status: %d", metricsReq.Code)
	}
	if !bytes.Contains(metricsReq.Body.Bytes(), []byte("exporter_progress 1")) {
		t.Fatalf("metrics missing progress: %s", metricsReq.Body.String())
	}

	svc2 := NewServiceWithLister(cfg, client, sink, lister)
	if err := svc2.Start(context.Background()); err != nil {
		t.Fatalf("restart: %v", err)
	}
	svc2.Wait()
	if len(sink.pages) != 8 {
		t.Fatalf("expected pages to be re-exported, got %d", len(sink.pages))
	}
}

func gzipData(data []byte) []byte {
	buf := &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	gz.Write(data)
	gz.Close()
	return buf.Bytes()
}
