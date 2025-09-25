package exporter

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
)

func NewHTTPDumpLister(baseURL string, client *http.Client) DumpLister {
	// Trim the trailing slash so url.JoinPath doesn't introduce double separators
	// when we later append file names. A bare root URL ("/") remains untouched.
	base := strings.TrimSuffix(baseURL, "/")
	return &httpLister{base: base, client: client}
}

type httpLister struct {
	base   string
	client *http.Client
}

func (l *httpLister) List(ctx context.Context) ([]DumpFile, error) {
	links, err := l.fetchLinks(ctx, l.base+"/")
	if err != nil {
		return nil, err
	}

	datasets := make(map[string]map[string]DumpFile)
	for _, link := range links {
		file, ok := parseDumpLink(link)
		if !ok {
			continue
		}
		abs, err := url.JoinPath(l.base, file.Name)
		if err != nil {
			log.Printf("build dump url failed for %s: %v", file.Name, err)
			continue
		}
		file.URL = abs

		versions := datasets[file.Dataset]
		if versions == nil {
			versions = make(map[string]DumpFile)
			datasets[file.Dataset] = versions
		}
		versions[file.Version] = file
	}

	latestByDataset := make([]DumpFile, 0, len(datasets))
	for _, versions := range datasets {
		keys := make([]string, 0, len(versions))
		for version := range versions {
			keys = append(keys, version)
		}
		if len(keys) == 0 {
			continue
		}
		latestByDataset = append(latestByDataset, versions[slices.Max(keys)])
	}

	langMax := make(map[string]string, len(latestByDataset))
	for _, file := range latestByDataset {
		if current, ok := langMax[file.Language]; !ok || current < file.Version {
			langMax[file.Language] = file.Version
		}
	}

	files := make([]DumpFile, 0, len(latestByDataset))
	for _, file := range latestByDataset {
		if file.Version == langMax[file.Language] {
			files = append(files, file)
		}
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].Language == files[j].Language {
			return files[i].Name < files[j].Name
		}
		return files[i].Language < files[j].Language
	})

	return files, nil
}

func parseDumpLink(name string) (DumpFile, bool) {
	if name == "" || strings.Contains(name, "/") {
		return DumpFile{}, false
	}
	lower := strings.ToLower(name)
	if !strings.HasPrefix(lower, "wikipedia_") || !strings.HasSuffix(lower, ".zim") {
		return DumpFile{}, false
	}
	if !strings.Contains(lower, "_nopic_") || strings.Contains(lower, "_all_nopic_") {
		return DumpFile{}, false
	}
	dataset, version, ok := splitDatasetVersion(name)
	if !ok {
		return DumpFile{}, false
	}
	lang := datasetLanguage(dataset)
	if lang == "" {
		return DumpFile{}, false
	}
	return DumpFile{Name: name, Language: lang, Dataset: dataset, Version: version}, true
}

func splitDatasetVersion(name string) (dataset, version string, ok bool) {
	base, hasSuffix := strings.CutSuffix(name, ".zim")
	if !hasSuffix {
		return "", "", false
	}
	idx := strings.LastIndex(base, "_")
	if idx < 0 {
		return "", "", false
	}
	dataset, version = base[:idx], base[idx+1:]
	if dataset == "" || version == "" || !isValidVersion(version) {
		return "", "", false
	}
	return dataset, version, true
}

func isValidVersion(version string) bool {
	for _, r := range version {
		if r != '-' && (r < '0' || r > '9') {
			return false
		}
	}
	return true
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
		if cleaned := cleanLink(m[1]); cleaned != "" {
			links = append(links, cleaned)
		}
	}
	return links, nil
}

var linkPattern = regexp.MustCompile(`href="([^"]+)"`)

func cleanLink(link string) string {
	link = strings.TrimSpace(link)
	switch {
	case link == "":
		return ""
	case strings.HasPrefix(link, "http://"), strings.HasPrefix(link, "https://"), strings.HasPrefix(link, "//"):
		return ""
	case strings.HasPrefix(link, "#"):
		return ""
	}
	if strings.Contains(link, "../") {
		return ""
	}
	if idx := strings.IndexAny(link, "?#"); idx >= 0 {
		link = link[:idx]
	}
	return strings.TrimPrefix(link, "./")
}

func datasetLanguage(name string) string {
	parts := strings.Split(name, "_")
	if len(parts) < 3 || parts[0] != "wikipedia" {
		return ""
	}
	return parts[1]
}
