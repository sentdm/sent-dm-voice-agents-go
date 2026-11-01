# Working in sent-dm-voice-agents-go

The Sent Voice Agents SDK for Go. Rendered by `sent-dm-sdk-pipeline` (`voice-agents/`) from
`sent-dm-voice-protocol` at a pinned tag; the lane regenerates the generated parts on a protocol tag.

## What is generated and what is written

- **Generated, do not edit**: `frames.go`. A change there is a change to the protocol or to the lane's emitter
  in `sent-dm-sdk-pipeline`.
- **Rendered from the lane's template**: everything else. A fix here is welcome; the lane carries it back into
  the template so every rendered repo gets it.
- **release-please-owned**: `CHANGELOG.md`, `.release-please-manifest.json`, the version in `version.go`.

## Rules

- The protocol's conformance scenarios run in CI on every PR, against `./bin/conformance` built from
  `cmd/conformance`. A PR that fails them does not merge; a release never ships without them green.
- The runtime follows `docs/protocol.md` in the protocol repository, not the other way round. When the protocol
  and the code disagree, the protocol is right and the code changes.
- Never log a resume token or an API key.
- `gofmt -l .` is empty and `go vet ./...` is clean on every PR; the generated file is gofmt-clean too.
- Branches named `release-please--branches--main` belong to release-please. Don't push to them or merge into
  them. Merging the release PR tags the release; the tag on this public repository is what `go get` fetches,
  and the publish workflow makes the Go module proxy index it.

## Commits and pull requests

- **Conventional Commits drive release-please**: `feat:`, `fix:`, `chore:`, `docs:`. The subject becomes the
  changelog line customers read, so write it for them: what changed, and what to do about it.
- **`ci:` and `test:` are hidden from the changelog.** `chore:`, `docs:`, `feat:` and `fix:` are not, and any of
  them cuts a release when the release PR is merged.
- **PR titles carry no type prefix**: `voice-agents-go: <what changed>`. PRs land with a merge commit, and
  release-please reads a conventional PR title as one more commit, so a `feat:` title lists the change twice.
- **One logical change per PR.**
- **Never name the calling provider** in code, comments, docs, changelogs or release notes. Sent is the
  provider as far as this SDK is concerned.
