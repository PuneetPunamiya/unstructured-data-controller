/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pathfilter

import (
	"testing"
)

func TestNew_InvalidPatterns(t *testing.T) {
	tests := []struct {
		name  string
		rules []string
	}{
		{
			name:  "empty pattern after prefix strip",
			rules: []string{"!"},
		},
		{
			name:  "bare slash becomes empty",
			rules: []string{"!/"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.rules)
			if err == nil {
				t.Errorf("New(%v) expected error, got nil", tt.rules)
			}
		})
	}
}

func TestNew_ValidPatterns(t *testing.T) {
	tests := []struct {
		name  string
		rules []string
	}{
		{
			name:  "no rules",
			rules: nil,
		},
		{
			name:  "simple include",
			rules: []string{"docs/"},
		},
		{
			name:  "include and exclude",
			rules: []string{"docs/", "!vendor/"},
		},
		{
			name:  "doublestar glob",
			rules: []string{"**/*.md"},
		},
		{
			name:  "comment lines are skipped",
			rules: []string{"# this is a comment", "docs/"},
		},
		{
			name:  "empty lines are skipped",
			rules: []string{"", "  ", "docs/"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := New(tt.rules)
			if err != nil {
				t.Errorf("New(%v) unexpected error: %v", tt.rules, err)
			}
			if filter == nil {
				t.Error("New() returned nil")
			}
		})
	}
}

func TestFilter_Match(t *testing.T) {
	tests := []struct {
		name    string
		rules   []string
		path    string
		isDir   bool
		matched bool
	}{
		{
			name:    "no rules matches everything",
			rules:   nil,
			path:    "any/file.txt",
			isDir:   false,
			matched: true,
		},
		{
			name:    "include docs dir matches file under docs",
			rules:   []string{"docs/"},
			path:    "docs/guide.md",
			isDir:   false,
			matched: true,
		},
		{
			name:    "include docs dir does not match file outside docs",
			rules:   []string{"docs/"},
			path:    "src/main.go",
			isDir:   false,
			matched: false,
		},
		{
			name:    "include docs dir matches nested file",
			rules:   []string{"docs/"},
			path:    "docs/api/reference.md",
			isDir:   false,
			matched: true,
		},
		{
			name:    "include docs dir matches directory itself for descent",
			rules:   []string{"docs/"},
			path:    "docs",
			isDir:   true,
			matched: true,
		},
		{
			name:    "exclude vendor blocks files under vendor",
			rules:   []string{"!vendor/"},
			path:    "vendor/lib/module.go",
			isDir:   false,
			matched: false,
		},
		{
			name:    "exclude vendor allows files outside vendor",
			rules:   []string{"!vendor/"},
			path:    "src/main.go",
			isDir:   false,
			matched: true,
		},
		{
			name:    "exclude takes precedence over include",
			rules:   []string{"docs/", "!docs/internal/"},
			path:    "docs/internal/secret.md",
			isDir:   false,
			matched: false,
		},
		{
			name:    "exclude does not block sibling of excluded dir",
			rules:   []string{"docs/", "!docs/internal/"},
			path:    "docs/public/guide.md",
			isDir:   false,
			matched: true,
		},
		{
			name:    "glob pattern matches by extension",
			rules:   []string{"*.md"},
			path:    "README.md",
			isDir:   false,
			matched: true,
		},
		{
			name:    "glob pattern does not match other extensions",
			rules:   []string{"*.md"},
			path:    "main.go",
			isDir:   false,
			matched: false,
		},
		{
			name:    "doublestar exclude matches deep paths",
			rules:   []string{"!**/*.log"},
			path:    "src/deep/nested/app.log",
			isDir:   false,
			matched: false,
		},
		{
			name:    "doublestar exclude allows non-matching deep paths",
			rules:   []string{"!**/*.log"},
			path:    "src/deep/nested/app.go",
			isDir:   false,
			matched: true,
		},
		{
			name:    "multiple includes use OR logic",
			rules:   []string{"docs/", "src/"},
			path:    "src/main.go",
			isDir:   false,
			matched: true,
		},
		{
			name:    "multiple includes reject paths matching neither",
			rules:   []string{"docs/", "src/"},
			path:    "test/main_test.go",
			isDir:   false,
			matched: false,
		},
		{
			name:    "dirOnly exclude still blocks files under that dir",
			rules:   []string{"!build/"},
			path:    "build/output/binary",
			isDir:   false,
			matched: false,
		},
		{
			name:    "path with leading dot-slash is normalized",
			rules:   []string{"docs/"},
			path:    "./docs/guide.md",
			isDir:   false,
			matched: true,
		},
		{
			name:    "unrelated directory not matched for descent",
			rules:   []string{"docs/"},
			path:    "vendor",
			isDir:   true,
			matched: false,
		},
		{
			name:    "exact file include",
			rules:   []string{"README.md"},
			path:    "README.md",
			isDir:   false,
			matched: true,
		},
		{
			name:    "exact file include does not match other files",
			rules:   []string{"README.md"},
			path:    "CONTRIBUTING.md",
			isDir:   false,
			matched: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := New(tt.rules)
			if err != nil {
				t.Fatalf("New(%v) unexpected error: %v", tt.rules, err)
			}
			got := filter.Match(tt.path, tt.isDir)
			if got != tt.matched {
				t.Errorf("Match(%q, isDir=%v) = %v, want %v", tt.path, tt.isDir, got, tt.matched)
			}
		})
	}
}

func TestFilter_IsEmpty(t *testing.T) {
	emptyFilter, _ := New(nil)
	if !emptyFilter.IsEmpty() {
		t.Error("IsEmpty() = false for filter with no rules, want true")
	}

	nonEmptyFilter, _ := New([]string{"docs/"})
	if nonEmptyFilter.IsEmpty() {
		t.Error("IsEmpty() = true for filter with rules, want false")
	}
}
