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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("valid rules", func(t *testing.T) {
		f, err := New([]string{"docs/", "README.md", "!vendor/**"})
		require.NoError(t, err)
		assert.Len(t, f.includes, 2)
		assert.Len(t, f.excludes, 1)
	})

	t.Run("empty pattern returns error", func(t *testing.T) {
		_, err := New([]string{""})
		assert.Error(t, err)
	})

	t.Run("negation-only bang returns error", func(t *testing.T) {
		_, err := New([]string{"!"})
		assert.Error(t, err)
	})

	t.Run("invalid glob returns error", func(t *testing.T) {
		_, err := New([]string{"docs/[invalid"})
		assert.Error(t, err)
	})

	t.Run("nil rules", func(t *testing.T) {
		f, err := New(nil)
		require.NoError(t, err)
		assert.True(t, f.IsEmpty())
	})
}

func TestFilterMatch(t *testing.T) {
	tests := []struct {
		name  string
		rules []string
		path  string
		isDir bool
		want  bool
	}{
		// Empty rules — everything matches
		{
			name: "no rules matches file",
			path: "src/main.go", isDir: false, want: true,
		},
		{
			name: "no rules matches dir",
			path: "src", isDir: true, want: true,
		},

		// Include-only rules
		{
			name:  "include dir matches file under it",
			rules: []string{"docs/"},
			path:  "docs/intro.md", isDir: false, want: true,
		},
		{
			name:  "include dir matches nested file",
			rules: []string{"docs/"},
			path:  "docs/guide/intro.md", isDir: false, want: true,
		},
		{
			name:  "include dir rejects file outside",
			rules: []string{"docs/"},
			path:  "src/main.go", isDir: false, want: false,
		},
		{
			name:  "include specific file",
			rules: []string{"README.md"},
			path:  "README.md", isDir: false, want: true,
		},
		{
			name:  "include specific file rejects other",
			rules: []string{"README.md"},
			path:  "CHANGELOG.md", isDir: false, want: false,
		},
		{
			name:  "multiple includes",
			rules: []string{"docs/", "README.md"},
			path:  "README.md", isDir: false, want: true,
		},
		{
			name:  "glob include",
			rules: []string{"docs/**/*.md"},
			path:  "docs/guide/intro.md", isDir: false, want: true,
		},
		{
			name:  "glob include rejects non-matching",
			rules: []string{"docs/**/*.md"},
			path:  "docs/guide/intro.txt", isDir: false, want: false,
		},

		// Exclude-only rules
		{
			name:  "exclude dir blocks file under it",
			rules: []string{"!vendor/**"},
			path:  "vendor/lib.go", isDir: false, want: false,
		},
		{
			name:  "exclude dir allows file outside",
			rules: []string{"!vendor/**"},
			path:  "src/main.go", isDir: false, want: true,
		},
		{
			name:  "exclude globstar blocks deep path",
			rules: []string{"!**/test/**"},
			path:  "foo/bar/test/baz.go", isDir: false, want: false,
		},
		{
			name:  "exclude globstar allows non-matching",
			rules: []string{"!**/test/**"},
			path:  "foo/bar/baz.go", isDir: false, want: true,
		},

		// Mixed include + exclude
		{
			name:  "mixed: include passes, exclude blocks",
			rules: []string{"docs/", "!docs/internal/**"},
			path:  "docs/guide.md", isDir: false, want: true,
		},
		{
			name:  "mixed: exclude overrides include",
			rules: []string{"docs/", "!docs/internal/**"},
			path:  "docs/internal/secret.md", isDir: false, want: false,
		},

		// Exclude takes precedence
		{
			name:  "exclude wins over include on same path",
			rules: []string{"docs/", "!docs/**"},
			path:  "docs/guide.md", isDir: false, want: false,
		},

		// Globstar includes
		{
			name:  "globstar include matches root",
			rules: []string{"**/README.md"},
			path:  "README.md", isDir: false, want: true,
		},
		{
			name:  "globstar include matches nested",
			rules: []string{"**/README.md"},
			path:  "a/b/README.md", isDir: false, want: true,
		},

		// Directory matching for walk pruning
		{
			name:  "dir excluded by exclude pattern",
			rules: []string{"!vendor/**"},
			path:  "vendor", isDir: true, want: false,
		},
		{
			name:  "dir allowed when it contains includes",
			rules: []string{"docs/"},
			path:  "docs", isDir: true, want: true,
		},
		{
			name:  "unrelated dir skipped when includes exist",
			rules: []string{"docs/"},
			path:  "src", isDir: true, want: false,
		},
		{
			name:  "dir allowed for globstar include",
			rules: []string{"**/README.md"},
			path:  "a", isDir: true, want: true,
		},
		{
			name:  "parent dir allowed when child path is included",
			rules: []string{"docs/guide/intro.md"},
			path:  "docs", isDir: true, want: true,
		},

		// Dir-only exclude with trailing slash
		{
			name:  "dir-only exclude blocks the directory",
			rules: []string{"!vendor/"},
			path:  "vendor", isDir: true, want: false,
		},
		{
			name:  "dir-only exclude blocks files under dir",
			rules: []string{"!vendor/"},
			path:  "vendor/lib.go", isDir: false, want: false,
		},

		// Realistic scenario from user spec
		{
			name:  "realistic: docs file matches",
			rules: []string{"docs/", "README.md", "!vendor/**", "!node_modules/**", "!**/test/**"},
			path:  "docs/guide/intro.md", isDir: false, want: true,
		},
		{
			name:  "realistic: README matches",
			rules: []string{"docs/", "README.md", "!vendor/**", "!node_modules/**", "!**/test/**"},
			path:  "README.md", isDir: false, want: true,
		},
		{
			name:  "realistic: vendor excluded",
			rules: []string{"docs/", "README.md", "!vendor/**", "!node_modules/**", "!**/test/**"},
			path:  "vendor/lib/foo.go", isDir: false, want: false,
		},
		{
			name:  "realistic: node_modules excluded",
			rules: []string{"docs/", "README.md", "!vendor/**", "!node_modules/**", "!**/test/**"},
			path:  "node_modules/pkg/index.js", isDir: false, want: false,
		},
		{
			name:  "realistic: test dir under docs excluded",
			rules: []string{"docs/", "README.md", "!vendor/**", "!node_modules/**", "!**/test/**"},
			path:  "docs/test/foo.md", isDir: false, want: false,
		},
		{
			name:  "realistic: unrelated file excluded by includes",
			rules: []string{"docs/", "README.md", "!vendor/**", "!node_modules/**", "!**/test/**"},
			path:  "src/main.go", isDir: false, want: false,
		},

		// Leading ./ normalization
		{
			name:  "leading dot-slash normalized",
			rules: []string{"docs/"},
			path:  "./docs/intro.md", isDir: false, want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := New(tt.rules)
			require.NoError(t, err)
			got := f.Match(tt.path, tt.isDir)
			assert.Equal(t, tt.want, got, "Match(%q, isDir=%v)", tt.path, tt.isDir)
		})
	}
}

func TestIsEmpty(t *testing.T) {
	f, err := New(nil)
	require.NoError(t, err)
	assert.True(t, f.IsEmpty())

	f, err = New([]string{"docs/"})
	require.NoError(t, err)
	assert.False(t, f.IsEmpty())
}
