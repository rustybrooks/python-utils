#!/usr/bin/env python
import argparse
import os
import subprocess
import sys
import tomllib

import semver


def run_command(cmd, cwd=None, dry_run=False):
    if dry_run:
        print(f"Would run command: {' '.join(cmd)}")
        return

    r = subprocess.call(cmd, cwd=cwd)
    if r != 0:
        raise Exception(f"Command {cmd} failed with return code {r}")


def generate_new_version(version, is_alpha=False, git_rev=None):
    ver = semver.Version.parse(version)

    # If this isn't an alpha build, or if this is the *first* alpha build, bump the patch
    if is_alpha:
        new_ver = ver.next_version(part="prerelease")
    else:
        new_ver = ver.next_version(part="patch")

    return str(new_ver)


def publish(args: argparse.Namespace):
    # open pyproject file, read a few things, and update version
    pyproject_file = os.path.join(args.pkg_dir, "pyproject.toml")
    with open(pyproject_file, "rb") as f:
        pyproject = tomllib.load(f)

    old_version = pyproject["tool"]["poetry"]["version"]
    sources = pyproject["tool"]["poetry"]["source"]
    source_repo = next(source for source in sources if source["name"] == "codeartifact")
    git_rev = (
        subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
        .decode("utf-8")
        .strip()
    )
    new_version = generate_new_version(
        old_version, is_alpha=args.alpha, git_rev=git_rev
    )
    upload_repo_url = source_repo["url"].replace("simple/", "")
    sys.stdout.flush()

    print(
        f"current version={old_version}, new version={new_version} upload_repo_url=${upload_repo_url}"
    )

    # update version in pyproject using sed
    run_command(
        [
            "sed",
            "-i.old",
            f's/version = "{old_version}"/version = "{new_version}"/',
            pyproject_file,
        ],
        dry_run=args.dry_run,
    )

    # configure poetry and publish repo
    run_command(
        ["poetry", "config", "repositories.codeartifact", upload_repo_url],
        cwd=args.pkg_dir,
        dry_run=args.dry_run,
    )
    run_command(["poetry", "build"], cwd=args.pkg_dir, dry_run=args.dry_run)
    run_command(
        ["poetry", "publish", "-r", "codeartifact"],
        cwd=args.pkg_dir,
        dry_run=args.dry_run,
    )

    # add and commit pyproject
    run_command(
        ["git", "config", "user.name", "github-actions"],
        dry_run=args.dry_run,
    )
    run_command(
        ["git", "config", "user.email", "github-actions@github.com"],
        dry_run=args.dry_run,
    )
    run_command(
        ["git", "add", pyproject_file],
        dry_run=args.dry_run,
    )
    run_command(
        ["git", "commit", "-m", f"chore(release) {args.pkg_dir} version={new_version}"],
        dry_run=args.dry_run,
    )

    push_cmd = ["git", "push"]
    if args.branch:
        push_cmd += ["origin", args.branch]
    run_command(
        push_cmd,
        dry_run=args.dry_run,
    )


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--alpha", default=False, action="store_true", help="publish alpha version"
    )
    parser.add_argument(
        "--dry-run", default=False, action="store_true", help="Whether to do a dry run"
    )
    parser.add_argument("--branch", type=str, help="Branch to push to")
    parser.add_argument("pkg_dir", type=str, help="Path to package to publish")
    parsed_args = parser.parse_args(args)

    publish(parsed_args)


if __name__ == "__main__":
    main()
