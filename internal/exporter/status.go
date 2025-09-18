package exporter

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

type statusFile struct {
	path string
	mu   sync.Mutex
}

func newStatusFile(path string) *statusFile {
	return &statusFile{path: path}
}

func (s *statusFile) Load() (map[string]bool, error) {
	if s == nil || s.path == "" {
		return map[string]bool{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]bool{}, nil
		}
		return nil, err
	}
	defer file.Close()

	result := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		result[line] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *statusFile) Append(name string) error {
	if s == nil || s.path == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(name + "\n"); err != nil {
		return err
	}
	return nil
}
