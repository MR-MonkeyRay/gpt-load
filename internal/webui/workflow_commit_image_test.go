package webui

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommitImageWorkflowPublishesEveryPushUnderTheShortCommitID(t *testing.T) {
	const workflowPath = ".github/workflows/commit-image.yml"
	content := readRepositoryFile(t, workflowPath)

	// 每次 push 都构建，不带任何分支或 tag 过滤器；只有 PR 不触发。
	trigger := workflowTopLevelBlock(t, content, "on")
	if got := strings.Join(workflowSignificantYAMLLines(trigger), "\n"); got != "on:\n  push:" {
		t.Fatalf("commit image triggers = %q, want an unfiltered push trigger", got)
	}

	for _, required := range []string{
		checkoutActionRef,
		setupGoActionRef,
		qemuActionRef,
		buildxActionRef,
		dockerLoginActionRef,
		dockerBuildActionRef,
	} {
		if !strings.Contains(content, "uses: "+required) {
			t.Fatalf("commit image workflow does not use pinned action %s", required)
		}
	}

	job := workflowJobBlock(t, content, "publish-commit-image")
	for _, required := range []string{
		"runs-on: ubuntu-24.04",
		"contents: read",
		"packages: write",
		// 凭据隔离：不覆盖宿主机 Docker 凭据。
		"echo \"DOCKER_CONFIG=${docker_config}\" >> \"${GITHUB_ENV}\"",
		// 镜像地址跟随当前仓库的 owner/repo，不硬编码任何账号。
		`image="ghcr.io/${GITHUB_REPOSITORY,,}"`,
		// tag 是 commit 短 id，长度固定，不随 core.abbrev 变化。
		`tag="${GITHUB_SHA:0:7}"`,
		"target: prebuilt",
		"platforms: linux/amd64,linux/arm64",
		"push: true",
		`VERSION=${{ steps.image.outputs.tag }}`,
		"org.opencontainers.image.revision=${{ github.sha }}",
		"org.opencontainers.image.version=${{ steps.image.outputs.tag }}",
		".github/scripts/release-verify-image-revision.sh",
	} {
		if !strings.Contains(job, required) {
			t.Fatalf("commit image job does not contain %q", required)
		}
	}

	// 短 id 必须来自 commit，不能退回 tag/ref 命名；否则同一 commit 会有两个名字。
	if strings.Contains(content, "github.ref_name") {
		t.Fatal("commit image workflow names images after a ref instead of the commit id")
	}
	// 推送的 tag 必须由解析步骤唯一决定，不能另有一处拼接。
	if count := strings.Count(job, "tags: ${{ steps.image.outputs.image }}:${{ steps.image.outputs.tag }}"); count != 1 {
		t.Fatalf("commit image push tags declaration count = %d, want exactly 1", count)
	}
}

func TestCommitImageReferenceDerivesRegistryPathAndShortCommitID(t *testing.T) {
	content := readRepositoryFile(t, ".github/workflows/commit-image.yml")
	step := workflowStepBlock(t, content, "Resolve commit image reference")
	script := workflowMarkedScript(t, step, "commit-image-reference")
	scriptPath := filepath.Join(t.TempDir(), "resolve-commit-image.sh")
	if err := os.WriteFile(
		scriptPath,
		[]byte("#!/usr/bin/env bash\nset -euo pipefail\n"+script),
		0o700,
	); err != nil {
		t.Fatalf("write commit image reference script: %v", err)
	}

	const sha = "0123456789abcdef0123456789abcdef01234567"
	for _, test := range []struct {
		name       string
		sha        string
		repository string
		want       string
		wantError  bool
	}{
		{
			name:       "canonical repository",
			sha:        sha,
			repository: "tbphp/gpt-load",
			want:       "image=ghcr.io/tbphp/gpt-load\ntag=0123456",
		},
		{
			name:       "uppercase owner is normalized for the registry",
			sha:        sha,
			repository: "TBphp/GPT-Load",
			want:       "image=ghcr.io/tbphp/gpt-load\ntag=0123456",
		},
		{
			name:       "dotted and underscored repository name",
			sha:        sha,
			repository: "some-org/my.repo_name",
			want:       "image=ghcr.io/some-org/my.repo_name\ntag=0123456",
		},
		{
			name:       "abbreviated commit sha is rejected",
			sha:        "0123456",
			repository: "tbphp/gpt-load",
			wantError:  true,
		},
		{
			name:       "non hexadecimal commit sha is rejected",
			sha:        strings.Repeat("z", 40),
			repository: "tbphp/gpt-load",
			wantError:  true,
		},
		{
			name:       "repository without owner is rejected",
			sha:        sha,
			repository: "gpt-load",
			wantError:  true,
		},
		{
			name:       "repository with a space is rejected",
			sha:        sha,
			repository: "tbphp/gpt load",
			wantError:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "github-output")
			command := exec.Command("bash", scriptPath)
			command.Env = []string{
				"PATH=" + os.Getenv("PATH"),
				"GITHUB_SHA=" + test.sha,
				"GITHUB_REPOSITORY=" + test.repository,
				"GITHUB_OUTPUT=" + outputPath,
			}
			err := command.Run()
			if test.wantError {
				if err == nil {
					t.Fatalf("unexpected commit image reference for %q", test.repository)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve commit image reference: %v", err)
			}
			output, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("read github output: %v", err)
			}
			if got := strings.TrimSpace(string(output)); got != test.want {
				t.Fatalf("commit image reference = %q, want %q", got, test.want)
			}
		})
	}
}
