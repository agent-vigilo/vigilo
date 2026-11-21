#!/usr/bin/env bash
# Release publication calls this after the crate is available. The retained
# snapshot marker and current release manifest make a retry safe.
set -euo pipefail

readonly bucket=agentvigilo.com
readonly distribution=E3UIV4SDTZNCBE
readonly tooling="$PWD/web/scripts/docs-release.mjs"
readonly state="$(mktemp -d)"
trap 'git worktree remove --force "$state/release" 2>/dev/null || true; git worktree remove --force "$state/correction" 2>/dev/null || true; rm -rf "$state"' EXIT

get_optional() {
  local key=$1 output=$2
  if aws s3api get-object --bucket "$bucket" --key "$key" "$output" > /dev/null 2> "$state/s3-error"; then
    return 0
  fi
  if grep -Fq '(NoSuchKey)' "$state/s3-error"; then
    return 1
  fi
  cat "$state/s3-error" >&2
  exit 1
}

if ! get_optional docs-versions.json "$state/current.json"; then
  printf '%s\n' '{"schemaVersion":1,"releases":[],"archivedLines":[]}' > "$state/current.json"
fi

tag=${VIGILO_RELEASE_TAG:-}
if [[ -z "$tag" ]]; then
  tag="$(node -p 'require(process.argv[1]).releases[0]?.tag ?? ""' "$state/current.json")"
fi
if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "Select a published Vigilo release tag, such as v0.1.0." >&2
  exit 1
fi

gh api "repos/$GITHUB_REPOSITORY/releases/tags/$tag" \
  --jq 'select(.draft == false and .published_at != null) | .tag_name' | grep -Fxq "$tag"
commit="$(git rev-parse --verify "refs/tags/$tag^{commit}")"
if [[ -n "${VIGILO_RELEASE_SHA:-}" && "$commit" != "$VIGILO_RELEASE_SHA" ]]; then
  echo "The release tag no longer points to the commit that published the crate." >&2
  exit 1
fi

git worktree add --detach "$state/release" "$commit"
version="$(python3 "$state/release/scripts/releases/crate.py" --tag "$tag")"
curl --fail --silent --show-error --retry 5 --retry-all-errors --max-time 15 \
  --user-agent vigilo-docs-release --output /dev/null "https://crates.io/api/v1/crates/vigilo/$version"

policy=-
if [[ "${VIGILO_APPLY_ARCHIVE_POLICY:-false}" == true ]]; then
  policy=web/docs-release-policy.json
fi
node "$tooling" plan "$state/current.json" "$policy" "$state/next.json" "$version" "$tag" "$commit" > "$state/plan"
read -r latest < <(sed -n 's/^latest=//p' "$state/plan")
read -r refresh_latest < <(sed -n 's/^refresh_latest=//p' "$state/plan")

root_source="$state/release"
if [[ -n "${VIGILO_DOCS_CORRECTION_SHA:-}" ]]; then
  correction=$VIGILO_DOCS_CORRECTION_SHA
  if [[ "$version" != "$latest" || ! "$correction" =~ ^[0-9a-f]{40}$ ]]; then
    echo "A docs correction requires the latest release and an exact commit SHA." >&2
    exit 1
  fi
  git merge-base --is-ancestor "$commit" "$correction"
  if ! git diff --quiet "$commit" "$correction" -- . ':(exclude)web/**'; then
    echo "A docs correction may only change files under web/ from the release tag." >&2
    exit 1
  fi
  git worktree add --detach "$state/correction" "$correction"
  root_source="$state/correction"
  refresh_latest=true
fi

build() {
  local source=$1 archive=$2 output=$3
  (
    cd "$source/web"
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN GH_TOKEN
    unset ACTIONS_ID_TOKEN_REQUEST_URL ACTIONS_ID_TOKEN_REQUEST_TOKEN
    npm ci
    VIGILO_DOCS_RELEASE="$version" VIGILO_DOCS_ARCHIVE="$archive" npm run build
    mv build "$output"
  )
}

prefix="releases/$version"
if get_optional "$prefix/snapshot.json" "$state/snapshot.json"; then
  node "$tooling" verify-snapshot "$state/snapshot.json" "$version" "$tag" "$commit"
else
  build "$state/release" 1 "$state/archive"
  if [[ ! -s "$state/archive/docs-routes.json" ]]; then
    echo "The release predates versioned documentation builds." >&2
    exit 1
  fi
  aws s3 sync "$state/archive/" "s3://$bucket/$prefix/" \
    --cache-control 'public,max-age=31536000,immutable' --only-show-errors
  node "$tooling" snapshot "$state/snapshot.json" "$version" "$tag" "$commit"
  aws s3api put-object --bucket "$bucket" --key "$prefix/snapshot.json" \
    --body "$state/snapshot.json" --content-type application/json > /dev/null
fi

if [[ "$refresh_latest" == true ]]; then
  build "$root_source" 0 "$state/latest"
  test -s "$state/latest/docs-routes.json"
  # Keep hashed assets for older in-flight pages. Archive prefixes and the
  # manifest are outside both current-site syncs.
  aws s3 sync "$state/latest/" "s3://$bucket/" --exclude '*.html' \
    --exclude 'releases/*' --exclude 'docs-versions.json' \
    --cache-control 'public,max-age=0,must-revalidate' --only-show-errors
  aws s3 sync "$state/latest/" "s3://$bucket/" --delete --exclude '*' --include '*.html' \
    --exclude 'releases/*' --exclude 'docs-versions.json' \
    --cache-control 'public,max-age=0,must-revalidate' --only-show-errors
fi

aws s3api put-object --bucket "$bucket" --key docs-versions.json --body "$state/next.json" \
  --content-type application/json --cache-control 'public,max-age=0,must-revalidate' > /dev/null
invalidation_id="$(aws cloudfront create-invalidation --distribution-id "$distribution" --paths '/*' \
  --query 'Invalidation.Id' --output text)"
aws cloudfront wait invalidation-completed --distribution-id "$distribution" --id "$invalidation_id"

fetch_public() {
  local key=$1 output=$2
  curl --fail --silent --show-error --location --retry 3 --retry-all-errors \
    --max-time 15 --retry-max-time 45 --output "$output" "https://$bucket/$key"
}

fetch_public docs-versions.json "$state/public-manifest.json"
fetch_public "$prefix/snapshot.json" "$state/public-snapshot.json"
fetch_public "$prefix/docs/guides/" "$state/public-archive.html"
fetch_public docs/ "$state/public-latest.html"
node "$tooling" verify-deployment "$state/next.json" "$version" "$state/public-manifest.json" \
  "$state/public-snapshot.json" "$state/public-archive.html" "$state/public-latest.html"
