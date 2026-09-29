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

package gitlabclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/pathfilter"
)

type Client struct {
	url      string
	revision string
	auth     *http.BasicAuth
}

type FileEntry struct {
	Path    string
	Hash    string
	Content []byte
}

func NewClient(url, revision, token string) *Client {
	c := &Client{
		url:      url,
		revision: revision,
	}
	if token != "" {
		c.auth = &http.BasicAuth{
			Username: "oauth2",
			Password: token,
		}
	}
	return c
}

// HeadHash fetches the current HEAD hash of the tracked revision via ls-remote.
// If the revision is already a full commit SHA, it is returned directly.
func (c *Client) HeadHash(ctx context.Context) (string, error) {
	if looksLikeSHA(c.revision) {
		return c.revision, nil
	}

	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{c.url},
	})

	refs, err := remote.ListContext(ctx, &git.ListOptions{Auth: c.auth})
	if err != nil {
		return "", fmt.Errorf("ls-remote failed for %s: %w", c.url, err)
	}

	candidates := []plumbing.ReferenceName{
		plumbing.NewBranchReferenceName(c.revision),
		plumbing.NewTagReferenceName(c.revision),
	}
	for _, targetRef := range candidates {
		for _, ref := range refs {
			if ref.Name() == targetRef {
				return ref.Hash().String(), nil
			}
		}
	}
	return "", fmt.Errorf("revision %q not found as branch or tag in %s", c.revision, c.url)
}

// CloneAndWalk performs a shallow clone (depth=1) into memory and returns
// files matching the given filter and file format extensions.
func (c *Client) CloneAndWalk(
	ctx context.Context, filter *pathfilter.Filter, fileFormats []string,
) ([]FileEntry, error) {
	fs := memfs.New()

	if looksLikeSHA(c.revision) {
		_, err := git.CloneContext(ctx, memory.NewStorage(), fs, &git.CloneOptions{
			URL:        c.url,
			NoCheckout: true,
			Auth:       c.auth,
		})
		if err != nil {
			return nil, fmt.Errorf("clone failed for %s: %w", c.url, err)
		}
		// TODO: checkout the specific SHA — go-git shallow clone by SHA
		// requires server support. For now, fall through to branch/tag clone.
		// This path will be revisited when SHA-based pinning is needed.
	}

	// Try as branch, then tag.
	cloneOpts := &git.CloneOptions{
		URL:           c.url,
		ReferenceName: plumbing.NewBranchReferenceName(c.revision),
		SingleBranch:  true,
		Depth:         1,
		Auth:          c.auth,
	}
	_, err := git.CloneContext(ctx, memory.NewStorage(), fs, cloneOpts)
	if err != nil {
		fs = memfs.New()
		cloneOpts.ReferenceName = plumbing.NewTagReferenceName(c.revision)
		_, err = git.CloneContext(ctx, memory.NewStorage(), fs, cloneOpts)
		if err != nil {
			return nil, fmt.Errorf("clone failed for %s (tried branch and tag %q): %w",
				c.url, c.revision, err)
		}
	}

	return walkFS(fs, ".", filter, fileFormats)
}

func walkFS(
	fs billy.Filesystem, dir string, filter *pathfilter.Filter, fileFormats []string,
) ([]FileEntry, error) {
	infos, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var entries []FileEntry
	for _, info := range infos {
		fullPath := filepath.Join(dir, info.Name())
		if fullPath == "." || strings.HasPrefix(fullPath, "."+string(filepath.Separator)) {
			fullPath = strings.TrimPrefix(fullPath, "."+string(filepath.Separator))
		}

		normalized := filepath.ToSlash(fullPath)

		if info.IsDir() {
			if info.Name() == ".git" {
				continue
			}
			if filter != nil && !filter.Match(normalized, true) {
				continue
			}
			sub, err := walkFS(fs, fullPath, filter, fileFormats)
			if err != nil {
				return nil, err
			}
			entries = append(entries, sub...)
			continue
		}

		if filter != nil && !filter.Match(normalized, false) {
			continue
		}

		if !matchesFileFormat(normalized, fileFormats) {
			continue
		}

		f, err := fs.Open(fullPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %w", fullPath, err)
		}
		content, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", fullPath, err)
		}

		h := sha256.Sum256(content)
		entries = append(entries, FileEntry{
			Path:    fullPath,
			Hash:    hex.EncodeToString(h[:]),
			Content: content,
		})
	}
	return entries, nil
}

// matchesFileFormat checks if a file has one of the specified extensions.
// Extensions are bare strings without dots (e.g. "md", "pdf").
// An empty list matches all files.
func matchesFileFormat(filePath string, formats []string) bool {
	if len(formats) == 0 {
		return true
	}
	ext := strings.TrimPrefix(filepath.Ext(filePath), ".")
	for _, f := range formats {
		if strings.EqualFold(ext, f) {
			return true
		}
	}
	return false
}

func looksLikeSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
