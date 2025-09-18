package exporter

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

const wikipediaDblistURL = "https://noc.wikimedia.org/conf/dblists/wikipedia.dblist"

func NewHTTPDumpLister(baseURL string, client *http.Client) DumpLister {
	return &httpLister{base: baseURL, client: client, dblistURL: wikipediaDblistURL}
}

type httpLister struct {
	base      string
	client    *http.Client
	dblistURL string
}

func (l *httpLister) List(ctx context.Context) (Inventory, error) {
	langs, err := l.loadLanguages(ctx)
	if err != nil {
		return Inventory{}, err
	}
	files := make([]DumpFile, 0)
	errs := make([]string, 0)
	for _, db := range langs {
		items, err := l.languageFiles(ctx, db)
		if err != nil {
			log.Printf("list %s failed: %v", db, err)
			errs = append(errs, fmt.Sprintf("%s: %v", db, err))
			continue
		}
		files = append(files, items...)
	}
	return Inventory{Languages: langs, Files: files, Errors: errs}, nil
}

func (l *httpLister) loadLanguages(ctx context.Context) ([]string, error) {
	log.Printf("fetching language list from %s", l.dblistURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.dblistURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", chromeUA)
	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("languages request failed: %s", resp.Status)
	}
	scanner := bufio.NewScanner(resp.Body)
	langs := make([]string, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		langs = append(langs, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Strings(langs)
	return langs, nil
}

func (l *httpLister) languageFiles(ctx context.Context, db string) ([]DumpFile, error) {
	if !strings.HasSuffix(db, "wiki") {
		return nil, nil
	}
	lang := strings.TrimSuffix(db, "wiki")
	langURL, err := url.JoinPath(l.base, db, "latest")
	if err != nil {
		return nil, err
	}
	log.Printf("fetching dump inventory for %s from %s/", db, langURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, langURL+"/", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", chromeUA)
	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list failed for %s: %s", db, resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	matches := linkPattern.FindAllStringSubmatch(string(body), -1)
	files := make([]DumpFile, 0)
	for _, m := range matches {
		name := sanitizeLink(m[1])
		if !isChunk(name) {
			continue
		}
		abs, err := url.JoinPath(langURL, name)
		if err != nil {
			continue
		}
		files = append(files, DumpFile{Name: name, URL: abs, Language: lang})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	return files, nil
}

var linkPattern = regexp.MustCompile(`href="([^"]+)"`)

func sanitizeLink(link string) string {
	if strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://") {
		return ""
	}
	if i := strings.LastIndex(link, "/"); i >= 0 {
		link = link[i+1:]
	}
	return link
}

func isChunk(name string) bool {
	if name == "" {
		return false
	}
	if !strings.Contains(name, "pages-articles") {
		return false
	}
	if strings.Contains(name, "multistream") {
		return false
	}
	if strings.HasSuffix(name, ".rss") {
		return false
	}
	if !strings.Contains(name, ".xml-p") {
		return false
	}
	return strings.HasSuffix(name, ".bz2") || strings.HasSuffix(name, ".gz")
}
