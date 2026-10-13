#!/bin/sh
# Collect per-platform release-asset download counts into a daily time series.
#
# GitHub reports `download_count` as a LIFETIME total per asset, so a daily
# figure only exists as a difference between two snapshots. This script records
# one snapshot per run; the deltas are computed at render time (see
# docs/install-stats/index.html) rather than stored, so a backfilled or
# corrected snapshot does not leave a stale delta behind it.
#
# No retroactive history is possible: the series begins at the first run.
#
# What the numbers do and do not mean
# -----------------------------------
# Platform is NOT a usable humans-vs-CI split, and release-smoke-linux.yaml says
# so itself: it installs "exactly as a real user would (curl | sh, downloading
# from GitHub Releases)". It is deliberately indistinguishable from a Linux user
# by behaviour. Treating linux_amd64 as "CI" would therefore erase every real
# Linux user -- confidently wrong rather than merely noisy.
#
# What the smoke test IS, is countable: ONE install per successful run, as far as
# these figures are concerned.
#
# The run itself installs three times -- a fresh install of the newest stable tag,
# an upgrade to the tag under test, and a no-op re-run of it -- so it fetches more
# than one checksums.txt. But on a push to main the tag under test is main-latest,
# which is excluded from .totals below (--clobber resets its download_count on
# every push, so its counts mean "since the last push" and its contribution is
# noise that can go negative when a real --ref=main install is erased). One fetch
# therefore lands where this page counts.
#
#   CI installs per run = 1
#
# The live figures settle it rather than the reading of the workflow: v0.8.1 has 71
# checksums fetches against 51 successful runs since it published. At 2 per run, CI
# alone would be 102 -- more than the release has ever been fetched. At 1 per run,
# 51 CI + 20 people = 71, and the Linux tarballs agree independently (112 / 2 = 56
# = 51 + 5).
#
# A TAG-push run is the exception: its upgrade leg lands on a real release, so the
# people figure can run 1-2 low on a day that cut a tag. Stated in the page
# footnote rather than modelled.
#
# Only `success` counts. Of 64 runs in the first sampled window, 18 were
# `skipped` and downloaded nothing, so counting runs naively overstates CI by
# ~28%.
#
# Nothing here separates maintainers from strangers -- at a base of ~16, a few
# contributors on a few machines each could account for all of it. Label any
# rendering accordingly; see the <h1> and the caveat block in index.html.
#
# Usage:
#   scripts/collect-install-stats.sh [--out PATH] [--repo OWNER/NAME]
#
# Requires: gh (authenticated), jq.

set -eu

OUT="docs/install-stats/data.json"
REPO="rossoctl/cortex"

while [ $# -gt 0 ]; do
	case "$1" in
	--out)
		[ $# -ge 2 ] || { echo "--out requires a value" >&2; exit 2; }
		OUT="$2"
		shift 2
		;;
	--repo)
		[ $# -ge 2 ] || { echo "--repo requires a value" >&2; exit 2; }
		REPO="$2"
		shift 2
		;;
	-h | --help)
		# Every comment line before the first line of code, rather than a
		# hardcoded range: a range silently stops matching when the header grows,
		# which is how --help came to end mid-sentence and never reach Usage.
		#
		# grep-then-stop rather than a sed range, so a blank line anywhere in or
		# after the header cannot truncate the output early: awk tracks whether it
		# is still in the leading block and exits at the first non-comment,
		# non-blank line.
		awk 'NR == 1 { next }
		     /^#/ { sub(/^# ?/, ""); print; next }
		     /^[[:space:]]*$/ { print ""; next }
		     { exit }' "$0"
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
done

warn() { echo "warning: $*" >&2; }

command -v gh >/dev/null 2>&1 || { echo "gh is required" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 1; }

today=$(date -u +%Y-%m-%d)
taken_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
tmp=$(mktemp)
snap=$(mktemp)
runs=$(mktemp)
latest=$(mktemp)
merged=$(mktemp)
trap 'rm -f "${tmp}" "${snap}" "${runs}" "${latest}" "${merged}"' EXIT INT TERM

# --paginate to cover every release, not just the first page: lifetime totals on
# older tags still move (mirrors, stragglers) and dropping them would silently
# understate the all-time figure.
gh api "repos/${REPO}/releases" --paginate >"${tmp}"

# Successful smoke runs, as {id, created_at} rather than per-day counts.
#
# Timestamps, not calendar dates, because a snapshot interval is not a day. The
# cron fires at 03:17 UTC, so a row covers (previous 03:17, this 03:17] -- and
# only 6 of 50 historical runs started before 03:17. Bucketing by calendar date
# therefore put ~88% of each row's runs in the wrong row, an error larger than the
# ~2.5 installs/day the row reports. Recording the timestamp lets the page bucket
# into the interval it actually drew its downloads from.
#
# ids, because snapshots are MERGED rather than replaced (see the jq below). The
# query returns the last 100 runs, which at ~10/day is about ten days, so a run
# visible today is gone within a fortnight -- and a page that read only the newest
# snapshot would see 0 for older intervals and subtract nothing, silently. Merging
# on id keeps each run exactly once, for as long as the series is kept.
#
# Queried by workflow FILE rather than by filtering every run in the repo: the
# repo-wide /actions/runs endpoint needs --paginate over thousands of runs and
# takes minutes, while this one returns the workflow's own runs in a single page.
SMOKE_WORKFLOW="release-smoke-linux.yaml"
if ! gh api \
	"repos/${REPO}/actions/workflows/${SMOKE_WORKFLOW}/runs?per_page=100" \
	--jq '{total_count: .total_count,
	       runs: [.workflow_runs[]
	              | select(.conclusion == "success")
	              | {id: .id, created_at: .created_at}]}' >"${runs}" 2>/dev/null; then
	# A missing or renamed workflow must not take the whole snapshot down: the
	# download series is the primary signal and stands on its own.
	#
	# null, NOT []: an empty list is a measurement ("no runs"), and a failed query
	# is not one. The page renders null as "—" and a list as a number, so the two
	# stay distinguishable instead of both reading as zero.
	warn "could not read ${SMOKE_WORKFLOW} runs; recording CI as unknown for today"
	echo 'null' >"${runs}"
fi

# GitHub's own answer to "which release does the plain installer take", recorded
# rather than derived. /releases/latest is the contract the installer follows, and
# it is the only thing that gets it right: sorting tags lexically puts v0.9.0 above
# v0.10.0, and sorting by date marks a v0.7.x backport published after v0.8.1 as
# current (release-0.6 and release-0.7 are both still live, so that is a real case
# here, not a hypothetical).
#
# `--jq .tag_name` would print a BARE string, which is not valid JSON for
# --slurpfile; `{tag_name}` keeps it an object the jq below can index.
if ! gh api "repos/${REPO}/releases/latest" --jq '{tag_name}' >"${latest}" 2>/dev/null; then
	warn "could not read the latest release; the page will mark no release current"
	echo 'null' >"${latest}"
fi

# Per-asset counts are collapsed into per-platform sums here rather than stored
# raw: keeping every asset name would grow the file without adding signal. The
# platform is kept because it says WHERE a release is being pulled, not who by --
# it cannot separate CI from people, for the reason in the header.
#
# checksums.txt is the primary install signal, and the reason is arithmetic: the
# installer fetches TWO tarballs per install (cortex + agentop) and verifies both
# against ONE checksums file. So a platform count double-counts an install while
# checksums counts it once, and anything derived from platform counts has to be
# halved to compare with it.
#
# That 1:2 ratio is stable across every release with traffic -- v0.8.1 66:133,
# v0.8.0 10:22, v0.7.0 18:37 -- which is what makes checksums trustworthy as an
# install count rather than a coincidence. A sustained drift away from ~2.0 means
# the installer changed what it fetches, or something is pulling tarballs
# directly (v0.7.0-alpha.7 reads 3.8x, which is the latter).
#
# (An earlier version of this comment claimed checksums EQUALLED the platform sum
# on v0.8.1. That was a mid-release snapshot read as a validating identity; it is
# 1:2, not 1:1.)
jq --arg date "${today}" \
   --arg taken_at "${taken_at}" \
   --slurpfile smoke "${runs}" \
   --slurpfile latest "${latest}" '
  def platform_of($name):
    if   $name | test("darwin_arm64") then "darwin_arm64"
    elif $name | test("darwin_amd64") then "darwin_amd64"
    elif $name | test("linux_arm64")  then "linux_arm64"
    elif $name | test("linux_amd64")  then "linux_amd64"
    # Unanchored: v0.1 through v0.4 named this file
    # kagenti-extensions_<ver>_checksums.txt, which an anchored ^checksums missed.
    # Those 24 releases were filed under "other" and read 0 installs apiece -- 79
    # downloads invisible, and the all-time total understated by 24% (257 against
    # 338). The platform tests above come first and no published asset has ever had
    # "checksums" in a tarball name, so widening this reclassifies nothing else.
    elif $name | test("checksums")    then "checksums"
    else "other"
    end;

  {
    date: $date,

    # The instant this snapshot was taken. The downloads in it accrued over
    # (previous taken_at, this taken_at], so this is what the page buckets CI runs
    # into. A calendar date cannot stand in for it: the cron fires at 03:17 UTC,
    # and almost every run lands after that.
    taken_at: $taken_at,

    # Successful smoke runs as {id, created_at}, or null when the query failed.
    # The page merges these across snapshots on id, so a run stays counted after it
    # falls out of the last-100 window the API returns.
    smoke_runs: ($smoke[0].runs // null),

    # How many runs the workflow has EVER had, successful or not, per the API.
    #
    # Lets the page tell a truncated history from a complete one. A page of 100 is
    # all the API gives, so when total_count is at or below 100 the first snapshot
    # holds the entire history for this workflow and no run is missing -- without
    # this, a release published before the workflow existed gets a floor marker for
    # runs that never happened.
    #
    # (No apostrophes in this jq program: it is single-quoted in the shell, so one
    # would end the quote and produce a confusing jq syntax error. Bitten 3x.)
    smoke_runs_total_count: ($smoke[0].total_count // null),

    # The tag GitHub serves as /releases/latest -- the release the plain installer
    # takes, and so the one CI installs. Recorded rather than derived: see the
    # query above for why neither tag sort nor date sort gets this right.
    latest_release: ($latest[0].tag_name // null),

    # Released tags only. A draft has no public download path, so counting it
    # would add a row that can never move.
    #
    # main-latest is dropped. release-binaries.yaml re-uploads it with --clobber on
    # every push to main, which RESETS download_count, so its counts mean "since the
    # last push" rather than lifetime. Summed into .totals it contributes noise
    # around zero that can go negative when a real --ref=main install is erased.
    releases: [
      .[]
      | select(.draft | not)
      | select(.tag_name != "main-latest")
      | {
        tag: .tag_name,
        published_at: .published_at,
        prerelease: .prerelease,
        platforms: (
          reduce (.assets[] | {p: platform_of(.name), c: .download_count}) as $a
            ({}; .[$a.p] = ((.[$a.p] // 0) + $a.c))
        ),
      }
    ],
  }
  # Totals are derived once here so every consumer agrees on them.
  | .totals = (
      reduce (.releases[].platforms | to_entries[]) as $e
        ({}; .[$e.key] = ((.[$e.key] // 0) + $e.value))
    )
' "${tmp}" >"${snap}"

mkdir -p "$(dirname "${OUT}")"
[ -f "${OUT}" ] || echo '{"snapshots":[]}' >"${OUT}"

# Re-running on the same day replaces that day's snapshot instead of appending a
# second one. Makes the job idempotent, so a manual workflow_dispatch after a
# failed cron run is safe and leaves exactly one row per date.
#
# Snapshots written before the checksums pattern was unanchored are repaired on the
# way past. Those misclassified downloads were counted -- just filed under "other"
# instead of "checksums" -- so moving them is exact arithmetic, not a guess, and it
# is what keeps the series continuous rather than showing a one-day jump of ~79 in a
# column whose daily deltas are 1-5.
#
# Keyed on the presence of "other": a snapshot taken after the fix has none, so this
# is a no-op from then on and needs no version flag or cutoff date. The per-release
# figures inside each snapshot are left alone -- they are what a future reader would
# recompute from, and they are already right under the new pattern.
jq --slurpfile snap "${snap}" '
  def repair:
    if (.totals.other // 0) > 0 then
      .totals.checksums = ((.totals.checksums // 0) + .totals.other)
      | del(.totals.other)
    else . end;

  .snapshots
  |= ( map(select(.date != $snap[0].date) | repair) + $snap
       | sort_by(.date) )
' "${OUT}" >"${merged}"

# cat rather than mv: OUT may live on a different filesystem than TMPDIR (it does
# under the Actions worktree), where mv across devices is not atomic anyway, and
# leaving the file in place keeps the trap responsible for removing it.
cat "${merged}" >"${OUT}"

days=$(jq '.snapshots | length' "${OUT}")
echo "recorded ${today} in ${OUT} (${days} day(s) of history)"
jq -r '.snapshots[-1]
       | "  cumulative: darwin_arm64=\(.totals.darwin_arm64 // 0)"
       + " linux_amd64=\(.totals.linux_amd64 // 0)"
       + " checksums=\(.totals.checksums // 0)",
         "  latest release: \(.latest_release // "unknown")",
         "  smoke runs in window: \(
            if .smoke_runs == null then "unknown (query failed)"
            else (.smoke_runs | length | tostring) end)"' "${OUT}"
