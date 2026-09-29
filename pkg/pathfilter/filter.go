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

	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// Filter evaluates file paths against a set of include/exclude rules.
// Excludes always take precedence over includes. If no include rules
// are specified, all paths are included by default (excludes still apply).
type Filter struct {
	includes    []gitignore.Pattern
	excludes    []gitignore.Pattern
	includeStrs []string
}

// New parses the given rules and returns a Filter.
// Each rule is a glob pattern. Rules prefixed with "!" are exclusions.
// Returns an error if any pattern is empty after stripping prefixes.
func New(rules []string) (*Filter, error) {
	f := &Filter{}
	for _, raw := range rules {
		raw = strings.TrimSpace(raw)
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		isExclude := strings.HasPrefix(raw, "!")
		pattern := raw
		if isExclude {
			pattern = raw[1:]
		}
		pattern = strings.TrimPrefix(pattern, "/")
		pattern = strings.TrimSuffix(pattern, "/")
		if pattern == "" {
			return nil, fmt.Errorf("empty pattern in rule %q", raw)
		}
		p := gitignore.ParsePattern(pattern, nil)
		if isExclude {
			f.excludes = append(f.excludes, p)
		} else {
			f.includes = append(f.includes, p)
			f.includeStrs = append(f.includeStrs, pattern)
		}
	}
	return f, nil
}

func patternMatches(p gitignore.Pattern, path string, isDir bool) bool {
	return p.Match(strings.Split(path, "/"), isDir) == gitignore.Exclude
}

// Match returns true if the given path should be included.
// The path should be slash-separated and relative to the repository root
// (no leading "./" or "/"). isDir indicates whether the path is a directory.
func (f *Filter) Match(path string, isDir bool) bool {
	path = strings.TrimPrefix(path, "./")
	path = strings.TrimSuffix(path, "/")

	for _, p := range f.excludes {
		if patternMatches(p, path, isDir) {
			return false
		}
	}

	if len(f.includes) == 0 {
		return true
	}

	for i, p := range f.includes {
		if patternMatches(p, path, isDir) {
			return true
		}
		if isDir && couldContainMatch(f.includeStrs[i], path) {
			return true
		}
	}
	return false
}

// couldContainMatch prevents premature directory pruning during tree walks.
func couldContainMatch(pattern, dirPath string) bool {
	if strings.HasPrefix(pattern, dirPath+"/") {
		return true
	}
	return strings.Contains(pattern, "**")
}

// IsEmpty returns true if no rules have been configured.
func (f *Filter) IsEmpty() bool {
	return len(f.includes) == 0 && len(f.excludes) == 0
}
