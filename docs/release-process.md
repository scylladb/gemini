# Gemini release process

1. Update `CHANGELOG.md` to match the new version.

2. Tag the release commit with `git tag`. We use the format `v1.2.3` for tag names.

3. Run `goreleaser` in `cmd/gemini` directory. It will ask you to configure AWS S3 and Github credentials as environment variables.
