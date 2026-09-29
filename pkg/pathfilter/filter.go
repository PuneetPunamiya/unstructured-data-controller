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
	"fmt"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
)

type rule struct {
	pattern string
	exclude bool
	dirOnly bool
}

// Filter evaluates file paths against a set of include/exclude rules.
// Excludes always take precedence over includes. If no include rules
// are specified, all paths are included by default (excludes still apply).
type Filter struct {
	includes []rule
	excludes []rule
}

// New parses the given rules and returns a Filter.
// Each rule is a glob pattern. Rules prefixed with "!" are exclusions.
// A trailing "/" on a rule means it applies only to directories.
// Returns an error if any pattern is syntactically invalid.
func New(rules []string) (*Filter, error) {
	f := &Filter{}
	for _, raw := range rules {
		r, err := parseRule(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid path rule %q: %w", raw, err)
		}
		if r.exclude {
			f.excludes = append(f.excludes, r)
		} else {
			f.includes = append(f.includes, r)
		}
	}
	return f, nil
}

// parseRule splits a raw path rule into its pattern, exclude flag, and dirOnly flag.
func parseRule(raw string) (rule, error) {
	var r rule
	s := raw

	if strings.HasPrefix(s, "!") {
		r.exclude = true
		s = s[1:]
	}

	if strings.HasSuffix(s, "/") {
		r.dirOnly = true
	}

	s = strings.TrimPrefix(s, "./")
	s = strings.TrimSuffix(s, "/")
	if s == "" {
		return rule{}, fmt.Errorf("empty pattern")
	}

	if !doublestar.ValidatePattern(s) {
		return rule{}, fmt.Errorf("invalid glob syntax")
	}

	r.pattern = s
	return r, nil
}

// Match returns true if the given path should be included.
// The path should be slash-separated and relative to the repository root
// (no leading "./" or "/"). isDir indicates whether the path is a directory.
func (f *Filter) Match(path string, isDir bool) bool {
	path = strings.TrimPrefix(path, "./")
	path = strings.TrimSuffix(path, "/")

	if f.matchesExclude(path, isDir) {
		return false
	}

	if len(f.includes) == 0 {
		return true
	}

	return f.matchesInclude(path, isDir)
}

// matchesExclude returns true if any exclude rule matches this path.
func (f *Filter) matchesExclude(path string, isDir bool) bool {
	for _, r := range f.excludes {
		if r.dirOnly && !isDir {
			// A dir-only exclude like "!vendor/" should still block files
			// under that directory via content matching.
			if matchesUnder(r.pattern, path) {
				return true
			}
			continue
		}
		if matchGlob(r.pattern, path) {
			return true
		}
		if !isDir && matchesUnder(r.pattern, path) {
			return true
		}
	}
	return false
}

// matchesInclude returns true if at least one include rule matches this path.
func (f *Filter) matchesInclude(path string, isDir bool) bool {
	for _, r := range f.includes {
		if isDir {
			// For directories, be conservative: allow descent if any
			// include rule could match files inside this directory.
			if couldContainMatch(r.pattern, path) {
				return true
			}
			continue
		}

		if r.dirOnly {
			if matchesUnder(r.pattern, path) {
				return true
			}
			continue
		}

		if matchGlob(r.pattern, path) {
			return true
		}
		if matchesUnder(r.pattern, path) {
			return true
		}
	}
	return false
}

// matchGlob performs a doublestar glob match.
func matchGlob(pattern, path string) bool {
	matched, _ := doublestar.Match(pattern, path)
	return matched
}

// matchesUnder checks if path falls under a directory described by pattern.
// E.g., pattern "docs" matches path "docs/guide/intro.md".
func matchesUnder(pattern, path string) bool {
	contentPattern := pattern + "/**"
	matched, _ := doublestar.Match(contentPattern, path)
	return matched
}

// couldContainMatch returns true if a directory at dirPath could contain
// files that match the given include pattern. This is used for directory
// pruning: we must not skip directories that might contain matching files.
func couldContainMatch(pattern, dirPath string) bool {
	// The directory itself matches the pattern.
	if matchGlob(pattern, dirPath) {
		return true
	}
	// The directory is under the pattern's tree.
	if matchesUnder(pattern, dirPath) {
		return true
	}
	// The pattern targets something under this directory.
	// e.g., pattern "docs/guide/intro.md" and dirPath "docs"
	if strings.HasPrefix(pattern, dirPath+"/") {
		return true
	}
	// The pattern uses ** which can match at any depth.
	if strings.Contains(pattern, "**") {
		return true
	}
	return false
}

// IsEmpty returns true if no rules have been configured.
func (f *Filter) IsEmpty() bool {
	return len(f.includes) == 0 && len(f.excludes) == 0
}
