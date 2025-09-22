package exporter

import (
	"encoding/json"
	"errors"
	"io"
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

func (s *statusFile) Load() (exportState, error) {
	if s == nil || s.path == "" {
		return exportState{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return exportState{}, nil
		}
		return exportState{}, err
	}
	defer file.Close()

	var state exportState
	if err := json.NewDecoder(file).Decode(&state); err != nil {
		if errors.Is(err, io.EOF) {
			return exportState{}, nil
		}
		return exportState{}, err
	}
	if state.Files == nil {
		state.Files = map[string]DumpFile{}
	}
	return state, nil
}

func (s *statusFile) Save(state exportState) error {
	if s == nil || s.path == "" {
		return nil
	}
	if state.Files == nil {
		state.Files = map[string]DumpFile{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), "status-*.tmp")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(tmp)
	if err := enc.Encode(state); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), s.path)
}
