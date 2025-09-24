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
	archives := map[string]*fakeZimArchive{
		"wikipedia_en_history_nopic_20240101.zim": {
			entries: []fakeZimEntry{
				{title: "Alpha", url: "A/Alpha", mime: "text/html", data: []byte("<html>alpha</html>")},
				{title: "Foo Bar", url: "A/Foo_Bar", mime: "text/html", data: []byte("<html>foo</html>")},
				{title: "Packed", url: "A/Packed", mime: "text/html", data: gzipBytes("<html>packed</html>")},
				{title: "README", url: "A/README", mime: "text/html", data: []byte("<html>ignore</html>")},
				{title: "Image", url: "I/image.png", mime: "image/png", data: []byte("binary")},
				{title: "notes", url: "A/notes.htm", mime: "text/html", data: []byte("<html>note</html>")},
			},
		},
		"wikipedia_ru_science_nopic_20240101.zim": {
			entries: []fakeZimEntry{
				{title: "Privet", url: "A/Privet", mime: "text/html", data: []byte("<html>privet</html>")},
				{title: "meta", url: "A/meta.json", mime: "application/json", data: []byte("{}")},
			},
		},
	}

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
		case "/kiwix/zim/wikipedia/":
			w.Write([]byte(`<a href="wikipedia_en_history_nopic_20240101.zim">en-history</a><a href="wikipedia_en_all_nopic_20240101.zim">skip-all</a><a href="wikipedia_ru_science_nopic_20240101.zim">ru-science</a><a href="speedtest_en_blob_2024-05.zim">speed</a>`))
		case "/kiwix/zim/wikipedia/wikipedia_en_history_nopic_20240101.zim":
			w.Write([]byte("zimdata"))
		case "/kiwix/zim/wikipedia/wikipedia_en_all_nopic_20240101.zim":
			http.Error(w, "should not fetch", http.StatusBadRequest)
		case "/kiwix/zim/wikipedia/wikipedia_ru_science_nopic_20240101.zim":
			w.Write([]byte("zimdata"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := server.Client()
	cfg := config.Config{
		DumpBaseURL: server.URL + "/kiwix/zim/wikipedia",
		WorkDir:     tmp,
		HTTPAddr:    ":0",
		Sink:        "stdout",
		StatusFile:  filepath.Join(tmp, "status.log"),
	}
	lister := NewHTTPDumpLister(cfg.DumpBaseURL, client)
	sink := &memorySink{}
	svc := NewServiceWithLister(cfg, client, sink, lister)
	svc.openZim = func(path string) (zimArchive, error) {
		name := filepath.Base(path)
		archive, ok := archives[name]
		if !ok {
			return nil, fmt.Errorf("unknown archive %s", name)
		}
		return archive, nil
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	svc.Wait()

	expected := map[string]string{
		"https://en.wikipedia.org/wiki/Alpha":   "<html>alpha</html>",
		"https://en.wikipedia.org/wiki/Foo_Bar": "<html>foo</html>",
		"https://en.wikipedia.org/wiki/notes":   "<html>note</html>",
		"https://ru.wikipedia.org/wiki/Privet":  "<html>privet</html>",
		"https://en.wikipedia.org/wiki/Packed":  "<html>packed</html>",
	}
	if len(sink.pages) != len(expected) {
		t.Fatalf("expected %d pages, got %d", len(expected), len(sink.pages))
	}
	for _, page := range sink.pages {
		if page.GetHttpCode() != 200 || page.GetIp() == 0 {
			t.Fatalf("unexpected page fields: %+v", page)
		}
		content := string(page.GetContent())
		want, ok := expected[page.GetSrcUrl()]
		if !ok {
			t.Fatalf("unexpected src url: %s", page.GetSrcUrl())
		}
		if content != want {
			t.Fatalf("unexpected content for %s: %q", page.GetSrcUrl(), content)
		}
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
	expectedFiles := []string{
		"wikipedia_en_history_nopic_20240101.zim",
		"wikipedia_ru_science_nopic_20240101.zim",
	}
	for _, name := range expectedFiles {
		if _, ok := state.Files[name]; !ok {
			t.Fatalf("status missing %s file: %+v", name, state.Files)
		}
	}
	for _, name := range expectedFiles {
		if _, err := os.Stat(filepath.Join(tmp, name)); err == nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dump file still present %s: %v", name, err)
		}
	}

	if agent := reqLog.agent["/kiwix/zim/wikipedia/"]; agent != chromeUA {
		t.Fatalf("unexpected index user agent: %q", agent)
	}
	if agent := reqLog.agent["/kiwix/zim/wikipedia/wikipedia_en_history_nopic_20240101.zim"]; agent != chromeUA {
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
	svc2.openZim = svc.openZim
	if err := svc2.Start(context.Background()); err != nil {
		t.Fatalf("restart: %v", err)
	}
	svc2.Wait()
	if len(sink.pages) != 10 {
		t.Fatalf("expected pages to be re-exported, got %d", len(sink.pages))
	}
}

type fakeZimArchive struct {
	entries []fakeZimEntry
}

func (f *fakeZimArchive) Close() error { return nil }

func (f *fakeZimArchive) Walk(ctx context.Context, fn func(zimEntry) error) error {
	for _, entry := range f.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

type fakeZimEntry struct {
	title    string
	url      string
	mime     string
	data     []byte
	redirect bool
	hasItem  *bool
}

func (f fakeZimEntry) Title() string         { return f.title }
func (f fakeZimEntry) URL() string           { return f.url }
func (f fakeZimEntry) MimeType() string      { return f.mime }
func (f fakeZimEntry) Data() ([]byte, error) { return f.data, nil }
func (f fakeZimEntry) IsRedirect() bool      { return f.redirect }
func (f fakeZimEntry) HasItem() bool {
	if f.hasItem != nil {
		return *f.hasItem
	}
	return len(f.data) > 0
}

func gzipBytes(body string) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(body)); err != nil {
		panic(err)
	}
	if err := zw.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
