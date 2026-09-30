#!/usr/bin/env python3
"""Fail closed if a private release would be exposed publicly.

Uses the workflow's short-lived GH_TOKEN; never logs credentials or tokens.
"""
import argparse
import json
import os
import urllib.error
import urllib.parse
import urllib.request


def api(path, missing_ok=False):
    request = urllib.request.Request(
        "https://api.github.com/" + path,
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"],
                 "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404 and missing_ok:
            return None
        raise RuntimeError(f"GitHub privacy check failed (HTTP {error.code})") from None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-package", action="store_true")
    parser.add_argument("--tag", help="Check anonymous access to this published manifest")
    args = parser.parse_args()
    repository = os.environ["GITHUB_REPOSITORY"]
    assert repository == "kritikmodi/neura", "This private publisher is scoped to kritikmodi/neura"
    repo = api("repos/" + repository)
    assert repo["private"] and repo["visibility"] == "private", "Repository must remain private"
    package = api("users/kritikmodi/packages/container/neura", missing_ok=not args.require_package)
    if package:
        assert package["visibility"] == "private", "Refusing to publish into a non-private package"
        assert package.get("repository", {}).get("full_name") == repository, "Package must be linked to this private repository"
    if args.tag:
        assert package is not None, "Cannot verify anonymity before the private package exists"
        url = "https://ghcr.io/token?" + urllib.parse.urlencode(
            {"service": "ghcr.io", "scope": "repository:kritikmodi/neura:pull"})
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                token = json.load(response)["token"]
        except urllib.error.HTTPError as error:
            assert error.code in (401, 403), f"Unexpected anonymous token response: {error.code}"
        else:
            request = urllib.request.Request(
                "https://ghcr.io/kritikmodi/neura/manifests/" + urllib.parse.quote(args.tag, safe=""),
                headers={"Authorization": "Bearer " + token,
                         "Accept": "application/vnd.oci.image.index.v1+json,application/vnd.oci.image.manifest.v1+json"})
            try:
                with urllib.request.urlopen(request, timeout=30):
                    raise AssertionError("Private image is anonymously readable")
            except urllib.error.HTTPError as error:
                assert error.code in (401, 403), f"Unexpected anonymous manifest response: {error.code}"
    print("PASS: repository and package privacy" + ("; anonymous image access denied" if args.tag else ""))


if __name__ == "__main__":
    main()
