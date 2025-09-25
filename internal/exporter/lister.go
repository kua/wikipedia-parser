package exporter

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func NewHTTPDumpLister(baseURL string, client *http.Client) DumpLister {
	base := strings.TrimSuffix(baseURL, "/")
	return &httpLister{base: base, client: client}
}

type httpLister struct {
	base   string
	client *http.Client
}

func (l *httpLister) List(ctx context.Context) (Inventory, error) {
	links, err := l.fetchLinks(ctx, l.base+"/")
	if err != nil {
		return Inventory{}, err
	}
	latest := make(map[string]DumpFile)
	errs := make([]string, 0)
	for _, link := range links {
		file, ok := l.parseRootEntry(link)
		if !ok {
			continue
		}
		abs, err := url.JoinPath(l.base, file.Name)
		if err != nil {
			log.Printf("build dump url failed for %s: %v", file.Name, err)
			errs = append(errs, fmt.Sprintf("%s: %v", file.Name, err))
			continue
		}
		file.URL = abs
		current, exists := latest[file.Dataset]
		if !exists || versionLess(current.Version, file.Version) {
			latest[file.Dataset] = file
		}
	}

	files := make([]DumpFile, 0, len(latest))
	langSet := make(map[string]struct{}, len(latest))
	for _, file := range latest {
		files = append(files, file)
		langSet[file.Language] = struct{}{}
	}

	languages := make([]string, 0, len(langSet))
	for lang := range langSet {
		languages = append(languages, lang)
	}
	sort.Strings(languages)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Language == files[j].Language {
			return files[i].Name < files[j].Name
		}
		return files[i].Language < files[j].Language
	})
	return Inventory{Languages: languages, Files: files, Errors: errs}, nil
}

func (l *httpLister) parseRootEntry(link string) (DumpFile, bool) {
	if link == "" {
		return DumpFile{}, false
	}
	if strings.Contains(link, "/") {
		return DumpFile{}, false
	}
	lower := strings.ToLower(link)
	if !strings.HasPrefix(lower, "wikipedia_") {
		return DumpFile{}, false
	}
	if !strings.HasSuffix(lower, ".zim") {
		return DumpFile{}, false
	}
	if !strings.Contains(lower, "_nopic_") {
		return DumpFile{}, false
	}
	if strings.Contains(lower, "_all_nopic_") {
		return DumpFile{}, false
	}
	dataset, version, ok := splitDatasetVersion(link)
	if !ok {
		return DumpFile{}, false
	}
	lang := datasetLanguage(dataset)
	if lang == "" {
		return DumpFile{}, false
	}
	return DumpFile{Name: link, Language: lang, Dataset: dataset, Version: version}, true
}

func splitDatasetVersion(name string) (string, string, bool) {
	if !strings.HasSuffix(name, ".zim") {
		return "", "", false
	}
	base := strings.TrimSuffix(name, ".zim")
	idx := strings.LastIndex(base, "_")
	if idx <= 0 || idx >= len(base)-1 {
		return "", "", false
	}
	dataset := base[:idx]
	version := base[idx+1:]
	if dataset == "" || version == "" {
		return "", "", false
	}
	if !isValidVersion(version) {
		return "", "", false
	}
	return dataset, version, true
}

func isValidVersion(version string) bool {
	for _, r := range version {
		if r == '-' {
			continue
		}
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func normalizeVersion(version string) (int, bool) {
	if version == "" {
		return 0, false
	}
	digits := strings.Builder{}
	digits.Grow(len(version))
	for _, r := range version {
		if r == '-' {
			continue
		}
		if r < '0' || r > '9' {
			return 0, false
		}
		digits.WriteRune(r)
	}
	if digits.Len() == 0 {
		return 0, false
	}
	value, err := strconv.Atoi(digits.String())
	if err != nil {
		return 0, false
	}
	return value, true
}

func versionLess(current, candidate string) bool {
	if current == "" {
		return true
	}
	if candidate == "" {
		return false
	}
	curVal, okCur := normalizeVersion(current)
	candVal, okCand := normalizeVersion(candidate)
	if okCur && okCand {
		if curVal != candVal {
			return curVal < candVal
		}
		return len(current) < len(candidate)
	}
	return current < candidate
}

func (l *httpLister) fetchLinks(ctx context.Context, target string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
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
		return nil, fmt.Errorf("list failed for %s: %s", target, resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	matches := linkPattern.FindAllStringSubmatch(string(body), -1)
	links := make([]string, 0, len(matches))
	for _, m := range matches {
		cleaned := cleanLink(m[1])
		if cleaned == "" {
			continue
		}
		links = append(links, cleaned)
	}
	return links, nil
}

var linkPattern = regexp.MustCompile(`href="([^"]+)"`)

func cleanLink(link string) string {
	link = strings.TrimSpace(link)
	if link == "" {
		return ""
	}
	if strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://") || strings.HasPrefix(link, "//") {
		return ""
	}
	if strings.HasPrefix(link, "#") {
		return ""
	}
	if strings.Contains(link, "../") {
		return ""
	}
	if idx := strings.IndexAny(link, "?#"); idx >= 0 {
		link = link[:idx]
	}
	link = strings.TrimPrefix(link, "./")
	return link
}

func datasetLanguage(name string) string {
	parts := strings.Split(name, "_")
	if len(parts) < 3 {
		return ""
	}
	if parts[0] != "wikipedia" {
		return ""
	}
	return parts[1]
}
