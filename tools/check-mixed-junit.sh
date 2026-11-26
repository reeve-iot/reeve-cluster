#!/usr/bin/env bash

set -euo pipefail

f=${1:?usage: tools/check-mixed-junit.sh <junit.xml>}
[[ -f $f ]] || { echo "$f does not exist: the test run wrote no result"; exit 1; }
for t in \
  A_write_acknowledged_by_one_version_is_applied_by_the_other \
  Owned_units_have_one_owner_while_placement_crosses_versions \
  Ask_is_answered_across_versions_in_both_directions \
  Claims_and_slots_exclude_across_versions \
  The_old_build_runs_on_the_schema_the_new_build_migrated \
  The_version_floor_is_refused_beside_the_old_build_and_raised_once_it_leaves \
  A_full_stop_rollback_lowers_the_floor_and_the_old_build_serves_again; do
  grep -q "name=\"$t\"" "$f" || { echo "$t did not run"; exit 1; }
  if grep -A1 "name=\"$t\"" "$f" | grep -q '<skipped'; then echo "$t was skipped"; exit 1; fi
done
