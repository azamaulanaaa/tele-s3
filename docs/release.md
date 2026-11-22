# Release workflow

This document describes the release workflow. The repository uses
[`Cocogitto`](https://docs.cocogitto.io/) to check commits, write the
changelog, and bump the version. The command name is `cog`.

## Commit format

Write every commit subject in this form:

```text
<type>(<scope>): <description>
```

`<type>` is one of these values: `feat`, `fix`, `docs`, `test`,
`refactor`, `perf`, `build`, `ci`, `chore`.

`<scope>` is optional. Use a short name for the changed area, for
example `ci` or `hooks`. `<description>` is a short sentence in the
imperative mood. Do not end the subject with a period.

A commit that breaks compatibility adds `!` before the colon:

```text
feat(api)!: remove the old flag
```

A `BREAKING CHANGE:` footer also marks a breaking change.

## Enforcement

The repository enforces the commit format in two places:

- `scripts/check-commits.sh` runs `cog check` from `scripts/ci.sh`.
  `make ci` and both Git hooks run this script.
- `.githook/pre-push` runs `scripts/ci.sh` before each push. A bad
  commit in the history stops the push.

Do not use `--no-verify` to skip these checks. The
[check pipeline](ci.md) describes the scripts.

## Changelog

`cog bump` generates `CHANGELOG.md`. Do not edit the file by hand. The
Markdown check skips this file. The file has no entry until the first
release.

## Versioning

`cog` selects the next version from the commit types:

- `fix` bumps PATCH.
- `feat` bumps MINOR.
- `!` or `BREAKING CHANGE:` bumps MAJOR.

Tags have the `v` prefix, for example `v1.2.3`.

`cog bump --auto` never jumps from `0.y.z` to `1.0.0`. The `0.y.z`
series marks a version before the first stable release.

## Make a release

Run the release on the `main` branch. `cog.toml` permits a bump on that
branch only. Run this command:

```bash
make release
```

The target runs `cog bump --auto` through `scripts/release.sh`. `--auto`
is the default selector. A version flag replaces `--auto`. When smolvm is
available, the bump runs in a writable guest clone. The command has three
steps:

1. It updates `CHANGELOG.md` and the crate version in `Cargo.toml` and `Cargo.lock`.
2. It creates a version commit.
3. It creates a tag with the `v` prefix.

Push `main` and then push the tag:

```bash
git push origin main
git push origin v1.2.3
```

The tag starts `.github/workflows/release.yml`. The workflow runs the
checks in `.github/workflows/ci.yml`, builds the cargo-dist artifacts,
uploads them, and creates the GitHub Release. The `release: published`
event starts `.github/workflows/release-container-image.yaml`. That
workflow publishes the container image for the tag. For a stable release,
it also publishes the `latest` tag.

The version bump runs on the host or in the smolvm guest through
`scripts/release.sh`. The artifact build and the container publish run in
CI on the tag and on the release.

## Sandbox release flow

`scripts/release.sh` uses the same execution modes as the checks:

- `SMOLVM_ENABLED=0` runs `cog bump --auto` on the host. The script
  uses no sandbox.
- `SMOLVM_ENABLED=1` requires smolvm. The script uses a local machine
  or a remote server. When neither is available, it prints an error and
  exits 3.
- An unset value or any other value selects auto mode. The script uses
  the remote server when `SMOLVM_ADDR` is set. It uses a local machine
  when `smolvm` is on `PATH` and the accelerator works. Otherwise it
  prints a warning and runs `cog bump --auto` on the host.

Local mode needs a normal checkout, not a linked worktree. The
repository mount must contain the git directory. Remote mode stages the
git data and can use a linked worktree.

In local mode, the script mounts the repository at `/workspace` with
the `:ro` flag. The mount stays read-only. The guest runs
`scripts/release-guest.sh`. That script does these steps:

1. It clones `/workspace` with `git clone --no-hardlinks` to a
    writable temp directory.
2. It removes the `origin` remote.
3. It checks out the host branch at the captured HEAD.
4. It sets the hooks path, the user name, and the user email.
5. It runs `cog bump` with the arguments from the host.

The guest runs no second sandbox. The pre-commit hook runs
`scripts/ci.sh` with `SMOLVM_ENABLED=0`, so the checks do not recurse.

The guest builds a git bundle. The bundle holds the new version commit
and the tag. The guest prints the bundle as base64 between two markers.
The host reads the markers and removes them from the printed output.

Before the host changes a ref, it checks these items:

- The bundle verifies.
- The bundle holds exactly one `v*` tag.
- The tag does not exist on the host.
- The commit has one parent, and the parent is the captured HEAD.
- The commit adds one commit.
- The commit changes only `CHANGELOG.md`, `Cargo.toml`, and `Cargo.lock`.
- The author and committer email match the host email.
- The host HEAD, branch, and worktree still match the preflight state.

The host fetches the tag from the bundle. It applies the commit with
`git merge --ff-only`. It creates no merge commit. A failure after the
fetch deletes the fetched tag. It resets the host to the captured HEAD
only when the host is on the captured branch at the guest commit and
the worktree is clean. Otherwise it leaves HEAD and the worktree alone.
A user commit or a dirty file from the guest run stays.

The tag stays local. Push it with `git push origin v1.2.3` when you
want the GitHub Release workflow to run.

A guest run that makes no commit prints `SMOLVM_RELEASE_NONE=1`. The
host prints a short message and exits 0. This covers `--dry-run` and a
history with no releasable commits.

Remote mode uses `scripts/smolvm-serve.sh` and the `serve_run_remote`
helper. It uses no new transport. The guest command has the same shape.
The remote server needs `SMOLVM_OWNER` and a reachable address. See the
[smolvm documentation](smolvm.md#remote-mode).

## Optional GitHub Release

The `make release` flow does not create a GitHub Release. The workflow
`.github/workflows/release.yml` creates the release when you push the
version tag. Push the tag that `make release` created:

```bash
git push origin v1.2.3
```

The tag must match `v*`, for example `v1.2.3`. The workflow runs the
`ci.yml` checks, builds the cargo-dist artifacts, uploads them, and
creates a GitHub Release for that tag. The changelog gives the release
notes. The `release: published` event then starts
`.github/workflows/release-container-image.yaml`, which publishes the
container image.

To keep the tag local, do not push it. The workflow does not run. No
GitHub Release is created and no container image is published.

## Dry run

Run this command on the `main` branch to see the next version:

```bash
cog bump --dry-run --auto
```

The command prints the next version and changes nothing. It does not
write the changelog, create a commit, or create a tag.

Run the dry run through the release path to test the sandbox:

```bash
make release ARGS='--dry-run'
```

The guest makes no commit. It prints `SMOLVM_RELEASE_NONE=1`. The host
prints a short message and exits 0. The host repository does not change.

## First release

When the repository has no tag, select the first version before the
first release. Run one of these commands:

```bash
make release ARGS='--version 1.0.0'
make release ARGS='--version 0.1.0'
```

The maintainer decides the first version. Use `1.0.0` when the public
interface is stable. Use `0.1.0` when the interface can change. The
flags `--major`, `--minor`, and `--patch` also replace `--auto`.

## Limits

The release path passed a simulation with a fake `cog`. The remote
release path ran on `smolvm serve` v1.35.5 with `SMOLVM_SERVE_CACHE=0`.
The guest ran a cold init with real cocogitto 7.0.0. A live local
`smolvm machine run` release has not run. The pack build and cache reuse
path for a release is not tested.

## Troubleshooting

### `cog` is not on `PATH`

The `cog` command must be on `PATH`. The check prints this message when
the tool is absent:

```text
error: cog is not on PATH. Install cocogitto first.
```

With `SMOLVM_ENABLED=1`, the release runs `cog` inside the sandbox. In
remote mode, also set `SMOLVM_OWNER`.

### The commit check rejects a message

`scripts/ci.sh` runs `scripts/check-commits.sh`. That script runs
`cog check`. The `make ci` command and the `pre-commit` and `pre-push`
hooks run `scripts/ci.sh`. A bad message fails the check.

To correct the last commit, run this command:

```bash
git commit --amend
```

To correct an older commit, use an interactive rebase:

```bash
git rebase -i HEAD~<n>
```

Then run `make ci` again. Do not use `--no-verify`.

### The Markdown check skips `CHANGELOG.md`

`cog bump` generates the file. The file format can disagree with the
Markdown rules. `scripts/check-markdown.sh` skips the file on purpose.
Do not add the file to the check.
