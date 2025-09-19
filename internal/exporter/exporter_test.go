package exporter

import (
	"bytes"
	"compress/gzip"
	"context"
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
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := server.Client()
	cfg := config.Config{
		DumpBaseURL: server.URL,
		WorkDir:     tmp,
		HTTPAddr:    ":0",
		Sink:        "stdout",
		StatusFile:  filepath.Join(tmp, "status.log"),
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
	if sink.pages[0].GetSrcUrl() != "https://en.wikipedia.org/wiki/Foo_Bar" {
		t.Fatalf("unexpected src url: %s", sink.pages[0].GetSrcUrl())
	}
	if sink.pages[0].GetHttpCode() != 200 || sink.pages[0].GetIp() == 0 {
		t.Fatalf("unexpected page fields: %+v", sink.pages[0])
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
	if !bytes.Contains(data, []byte("enwiki-latest-pages-articles1.xml-p1p2.gz")) {
		t.Fatalf("status file missing entry: %s", string(data))
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
	if len(sink.pages) != 4 {
		t.Fatalf("expected no new pages on restart, got %d", len(sink.pages))
	}
}

func gzipData(data []byte) []byte {
	buf := &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	gz.Write(data)
	gz.Close()
	return buf.Bytes()
}
