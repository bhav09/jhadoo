#!/usr/bin/env python3
"""Script to automatically yank older versions of a package on PyPI, keeping only the latest N versions."""

import argparse
import base64
import json
import os
import re
import sys
import urllib.request
import urllib.error


def parse_version(v_str):
    """Parse version string for sorting, falling back to custom tuple parsing if packaging is missing."""
    try:
        from packaging.version import parse
        return parse(v_str)
    except ImportError:
        parts = re.findall(r'\d+', v_str)
        return tuple(int(p) for p in parts)


def get_pypi_releases(package_name):
    """Fetch release information from PyPI JSON API."""
    url = f"https://pypi.org/pypi/{package_name}/json"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            return data.get("releases", {})
    except urllib.error.HTTPError as e:
        print(f"Error fetching package info from PyPI: {e}", file=sys.stderr)
        sys.exit(1)


def _basic_auth_header(token):
    """Build PyPI-standard Basic auth header for API token."""
    encoded = base64.b64encode(f"__token__:{token}".encode("utf-8")).decode("ascii")
    return f"Basic {encoded}"


def yank_release(package_name, version, token, reason, dry_run=True):
    """Yank a specific release version on PyPI using PATCH /api/projects/{name}/{version}."""
    url = f"https://pypi.org/api/projects/{package_name}/{version}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": _basic_auth_header(token),
    }
    data = json.dumps({
        "yanked": True,
        "yanked_reason": reason,
    }).encode("utf-8")

    if dry_run:
        print(f"[Dry Run] Would yank {package_name}=={version} with reason: '{reason}'")
        return True

    req = urllib.request.Request(url, data=data, headers=headers, method="PATCH")
    try:
        with urllib.request.urlopen(req) as response:
            if response.status in (200, 204):
                print(f"✓ Successfully yanked {package_name}=={version}")
                return True
            print(f"✗ Failed to yank {package_name}=={version}: HTTP {response.status}", file=sys.stderr)
            return False
    except urllib.error.HTTPError as e:
        try:
            err_detail = e.read().decode()
        except Exception:
            err_detail = str(e)

        if e.code in (404, 405):
            print(
                f"✗ PyPI programmatic yanking API returned {e.code} for {package_name}=={version}.",
                file=sys.stderr,
            )
            print(
                f"  Manual yank required: https://pypi.org/manage/project/{package_name}/releases/",
                file=sys.stderr,
            )
            return False

        print(f"✗ HTTP Error yanking {package_name}=={version}: {e.code} - {err_detail}", file=sys.stderr)
        return False


def verify_yank_state(package_name, keep=3):
    """Verify all versions outside the latest N are fully yanked on PyPI."""
    releases = get_pypi_releases(package_name)
    if not releases:
        print(f"No releases found for package '{package_name}'.", file=sys.stderr)
        return False

    sorted_versions = sorted(releases.keys(), key=parse_version, reverse=True)
    keep_versions = sorted_versions[:keep]
    offenders = []

    for version in sorted_versions[keep:]:
        files = releases.get(version) or []
        if not files:
            continue
        if not all(f.get("yanked", False) for f in files):
            offenders.append(version)

    print(f"Keeping un-yanked: {', '.join(keep_versions) or '(none)'}")
    if offenders:
        print(f"✗ The following versions should be yanked but are still active:", file=sys.stderr)
        for v in offenders:
            print(f"  - {v}", file=sys.stderr)
        print(
            f"  Manual yank: https://pypi.org/manage/project/{package_name}/releases/",
            file=sys.stderr,
        )
        return False

    print(f"✓ All versions older than the latest {keep} are yanked.")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Yank older versions of a PyPI package, keeping only the latest N versions.",
    )
    parser.add_argument("--package", default="jhadoo", help="Name of the PyPI package (default: jhadoo)")
    parser.add_argument("--keep", type=int, default=3, help="Number of recent versions to keep (default: 3)")
    parser.add_argument("--apply", action="store_true", help="Actually apply the yanking (default is dry-run)")
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify yank state via JSON API (no PATCH requests)",
    )
    parser.add_argument("--token", help="PyPI API token (can also be set via PYPI_API_TOKEN env var)")

    args = parser.parse_args()

    if args.verify_only:
        sys.exit(0 if verify_yank_state(args.package, args.keep) else 1)

    token = args.token or os.environ.get("PYPI_API_TOKEN")
    if not args.apply:
        print("Running in DRY-RUN mode. Pass --apply to execute changes on PyPI.")
    elif not token:
        print(
            "Error: PyPI API token is required when --apply is set. Use --token or set PYPI_API_TOKEN env var.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Fetching release history for '{args.package}' from PyPI...")
    releases = get_pypi_releases(args.package)

    if not releases:
        print(f"No releases found for package '{args.package}'.", file=sys.stderr)
        sys.exit(1)

    sorted_versions = sorted(releases.keys(), key=parse_version, reverse=True)
    print(f"Found {len(sorted_versions)} total versions.")

    keep_versions = sorted_versions[:args.keep]
    yank_candidates = sorted_versions[args.keep:]

    print(f"\nKeeping the following {len(keep_versions)} versions:")
    for v in keep_versions:
        print(f"  - {v}")

    print(f"\nChecking {len(yank_candidates)} older versions for yanking:")

    success_count = 0
    fail_count = 0
    skipped_count = 0
    api_unavailable = False

    for v in yank_candidates:
        files = releases[v]
        is_already_yanked = len(files) > 0 and all(f.get("yanked", False) for f in files)

        if is_already_yanked:
            print(f"  - {v}: Already yanked (Skipping)")
            skipped_count += 1
            continue

        reason = (
            f"Superseded — only the latest {args.keep} versions are supported. "
            f"Install {args.package}>={keep_versions[-1]}."
        )
        if yank_release(args.package, v, token, reason, dry_run=not args.apply):
            success_count += 1
        else:
            fail_count += 1
            if args.apply:
                api_unavailable = True
                break

    print(f"\nSummary:")
    print(f"  Total versions: {len(sorted_versions)}")
    print(f"  Kept: {len(keep_versions)}")
    print(f"  Already yanked (skipped): {skipped_count}")
    if args.apply:
        print(f"  Successfully yanked: {success_count}")
        print(f"  Failed: {fail_count}")
    else:
        print(f"  Yank candidates (dry-run): {success_count}")

    if fail_count > 0 and args.apply:
        print(
            f"\nAutomated yanking failed. Verify state with:\n"
            f"  python scripts/yank_old_pypi_versions.py --verify-only --keep {args.keep}",
            file=sys.stderr,
        )
        if api_unavailable:
            verify_ok = verify_yank_state(args.package, args.keep)
            sys.exit(0 if verify_ok else 1)
        sys.exit(1)

    if args.apply:
        verify_ok = verify_yank_state(args.package, args.keep)
        sys.exit(0 if verify_ok else 1)

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
