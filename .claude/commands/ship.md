Commit, push, tag, and push tag for the current changes.

Follow these steps exactly:

## 1. Run all checks

Run the full verification suite. If any check fails, stop and fix the issues before proceeding.

```bash
maturin develop
pytest
cargo test --lib
cargo clippy --lib -- -D warnings
cargo fmt --check
ruff check python/
ruff format --check python/
ty check python/
```

## 2. Analyze changes

Run `git status` (no -uall flag), `git diff --staged`, `git diff`, and `git log --oneline -5` in parallel.

## 3. Suggest a new version

- Look at the latest git tag: `git tag --sort=-v:refname | head -1`
- Based on the nature of the changes, suggest the next version following **semantic versioning**:
  - **patch** (0.1.1 → 0.1.2): bug fixes, small tweaks, formatting, docs
  - **minor** (0.1.2 → 0.2.0): new features, new options, new CLI commands
  - **major** (0.2.0 → 1.0.0): breaking changes to public API, config format changes, renamed commands
- Present the suggested version and a one-line rationale to the user
- **Wait for the user to confirm or adjust** before proceeding

## 4. Bump version

Update the version number in both files to match the confirmed version (without the `v` prefix):

- `pyproject.toml` → `version = "X.Y.Z"`
- `Cargo.toml` (workspace) → `version = "X.Y.Z"`

## 5. Update CHANGELOG.md

The changelog follows the [Keep a Changelog](https://keepachangelog.com/) format. If `CHANGELOG.md` does not exist, create it. Read it before editing.

- Add a new `## [vX.Y.Z] - YYYY-MM-DD` section at the top (below the title), using the confirmed version
- Group changes using these categories (only include categories that apply):
  - `### Added` — new features
  - `### Changed` — changes to existing functionality
  - `### Fixed` — bug fixes
  - `### Removed` — removed features
- Each entry is a concise bullet point describing the change from a user perspective
- Do NOT include internal refactoring details unless they affect users

## 6. Stage, commit, and push

- Stage all relevant changed files including `CHANGELOG.md` (avoid secrets, .env, credentials)
- Write a **very short** commit message (under 50 chars, no scope prefix needed)
- Do NOT add any `Co-Authored-By` trailer
- Use this exact format:

```
git commit -m "the short message"
git push origin main
```

## 7. Tag and push tag

If the tag already exists (e.g. failed release), delete it locally and remotely first:

```bash
git tag -d vX.Y.Z
git push origin :refs/tags/vX.Y.Z
```

Then create and push the tag:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

## 8. Verify release

Check that the Release pipeline triggered by the tag is running:

```bash
gh run list --limit 5
```

Report the status to the user and provide the run URL if available.
