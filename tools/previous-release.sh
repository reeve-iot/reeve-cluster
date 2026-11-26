#!/usr/bin/env bash

set -euo pipefail

releases() {
  git tag --list 'v*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V || true
}

below() {
  { releases; echo "$1"; } | sort -V -u | awk -v v="$1" '$0 == v { if (prev != "") print prev; exit } { prev = $0 }'
}

usage() { echo "usage: tools/previous-release.sh vX.Y.Z | --head | --require-below OLD_REF NEW_REF | --self-test" >&2; return 64; }

previous() {
  local version=$1
  [[ $version =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]] || { usage; return $?; }
  { below "v${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.0"; if [[ ${BASH_REMATCH[3]} != 0 ]]; then below "$version"; fi; } | sort -V -u
}

version_of() {
  local ref=$1 version
  if [[ $ref =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then echo "${ref#v}"; return 0; fi
  version=$(git show "$ref:Directory.Build.props" 2>/dev/null | sed -n 's:.*<Version>\(.*\)</Version>.*:\1:p' | head -1)
  [[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "previous-release.sh: ref '$ref' names no release version (no vX.Y.Z tag, no <Version> in its Directory.Build.props)" >&2; return 64; }
  echo "$version"
}

head_previous() {
  local version
  version=$(sed -n 's:.*<Version>\(.*\)</Version>.*:\1:p' Directory.Build.props | head -1)
  [[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "previous-release.sh: Directory.Build.props has no release <Version> (got '$version')" >&2; return 64; }
  previous "v$version"
}

require_below() {
  [[ $# == 2 ]] || { usage; return $?; }
  local old new
  old=$(version_of "$1") && new=$(version_of "$2") || return 64
  if [[ $old == "$new" || $(printf '%s\n%s\n' "$old" "$new" | sort -V | tail -1) == "$old" ]]; then
    echo "mixed fleet refused: old ref '$1' is version $old and new ref '$2' is version $new; the old side must be a release strictly below the new build (an equal pair runs one release against itself and proves nothing: bump <Version> in Directory.Build.props, or name an earlier release)" >&2
    return 1
  fi
}

self_test() {
  local scratch failed=0
  scratch=$(mktemp -d)
  trap 'rm -rf "$scratch"' RETURN
  git -C "$scratch" init -q
  props() { printf '<Project><PropertyGroup><Version>%s</Version></PropertyGroup></Project>\n' "$1" > "$scratch/Directory.Build.props"; }
  props 3.0.0
  git -C "$scratch" add Directory.Build.props
  git -C "$scratch" -c user.name=t -c user.email=t@t commit -q -m release
  for tag in v1.0.0 v2.0.0 v2.2.0 v2.2.1 v2.9.0 v2.10.0 v3.0.0 v3.0.0-rc.1 v3.1.0-rc.2 v4.0.0 release-1 vnext; do
    git -C "$scratch" tag "$tag"
  done

  check() {
    local expected=$1 actual rc=0
    shift
    actual=$(cd "$scratch" && "$self" "$@" 2>/dev/null | tr '\n' ' ') || rc=$?
    actual=${actual% }
    if [[ $actual != "$expected" || $rc != 0 ]]; then
      echo "previous-release.sh $*: expected '$expected' (exit 0), got '$actual' (exit $rc)" >&2
      failed=1
    fi
  }
  refuse() {
    local want=$1 rc=0
    shift
    (cd "$scratch" && "$self" "$@" >/dev/null 2>&1) || rc=$?
    if [[ $rc != "$want" ]]; then echo "previous-release.sh $*: expected exit $want, got $rc" >&2; failed=1; fi
  }

  check "v2.9.0" v2.10.0
  check "v2.2.1" v2.9.0
  check "v2.10.0" v3.0.0
  check "v2.0.0 v2.2.0" v2.2.1
  check "v2.9.0 v2.10.0" v2.10.1
  check "v2.10.0 v3.0.0" v3.0.1
  check "v1.0.0 v2.0.0" v2.0.1
  check "" v1.0.0
  check "v1.0.0" v1.0.1
  check "v2.10.0" v3.0.0
  check "v3.0.0" v3.1.0
  refuse 64 v3.0.0-rc.1
  refuse 64 v2.10
  refuse 64 ""

  check "v2.10.0" --head
  props 3.0.1; check "v2.10.0 v3.0.0" --head
  props 3.1.0; check "v3.0.0" --head
  props 2.9.0; check "v2.2.1" --head
  props 3.0.0
  echo 'not a props file' > "$scratch/Directory.Build.props"; refuse 64 --head
  props 3.0.0

  check "" --require-below v2.10.0 HEAD
  refuse 1 --require-below v3.0.0 HEAD
  refuse 1 --require-below v4.0.0 HEAD
  refuse 1 --require-below v2.10.0 v2.9.0
  check "" --require-below v2.9.0 v2.10.0
  refuse 64 --require-below nowhere HEAD
  refuse 64 --require-below v2.9.0
  return $failed
}

self=$(realpath "${BASH_SOURCE[0]}")
case ${1:-} in
  --self-test) self_test && echo "previous-release.sh: self-test passed" ;;
  --head) head_previous ;;
  --require-below) shift; require_below "$@" ;;
  *) previous "${1:-}" ;;
esac
