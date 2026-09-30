#!/usr/bin/env python3
"""Promote tested immutable image digests after all release gates pass."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def main():
    version, commit, image = (os.environ[key] for key in ("VERSION", "GITHUB_SHA", "IMAGE"))
    assert re.fullmatch(r"\d+\.\d+\.\d+-alpha\.\d+", version)
    assert image == "ghcr.io/kritikmodi/neura"
    subprocess.run(["python3", "scripts/release-privacy.py", "--require-package"], check=True)
    records = [json.loads(Path(f"release-digests/{arch}.json").read_text()) for arch in ("amd64", "arm64")]
    for arch, record in zip(("amd64", "arm64"), records):
        assert record["arch"] == arch and record["version"] == version and record["commit"] == commit
        assert all(re.fullmatch(r"sha256:[a-f0-9]{64}", record[k]) for k in ("standard", "cube"))
    manifest = {"version": version, "commit": commit, "images": {}, "validated_architectures": records}
    notes = [f"# Neura {version}", "", f"Private Docker alpha built from `{commit}`.", "",
             "Both images support Linux AMD64 and ARM64. Registry login is required.", ""]
    for variant, suffix in (("standard", ""), ("cube", "-cube")):
        tag = image + ":" + version + suffix
        refs = [image + "@" + r[variant] for r in records]
        subprocess.run(["docker", "buildx", "imagetools", "create", "--tag", tag, *refs], check=True)
        raw = subprocess.check_output(["docker", "buildx", "imagetools", "inspect", "--raw", tag])
        index = json.loads(raw)
        platforms = {m.get("platform", {}).get("architecture") for m in index["manifests"] if m.get("platform", {}).get("os") == "linux"}
        assert platforms == {"amd64", "arm64"}, platforms
        # Registry digest is obtained from inspect, rather than hashing potentially
        # re-formatted JSON emitted by a CLI.
        inspected = run("docker", "buildx", "imagetools", "inspect", tag)
        digest = next(line.split()[1] for line in inspected.splitlines() if line.startswith("Digest:"))
        assert re.fullmatch(r"sha256:[a-f0-9]{64}", digest)
        subprocess.run(["docker", "pull", image + "@" + digest], check=True)
        assert run("docker", "run", "--rm", image + "@" + digest, "--version") == "neura " + version
        subprocess.run(["python3", "scripts/release-privacy.py", "--require-package", "--tag", version + suffix], check=True)
        manifest["images"][variant] = {"tag": tag, "digest": digest, "platforms": sorted(platforms)}
        notes += [f"- `{tag}`", f"  - Immutable reference: `{image}@{digest}`"]
    notes += ["", "Validated: unit/static checks; actual PostgreSQL, ClickHouse, S3-compatible and Iceberg integrations;",
              "native AMD64/ARM64 packaged CLI, REST authentication, source RLS, PostgreSQL × S3 federation and Cube driver compatibility.",
              "BuildKit provenance and SBOM attestations are attached to the architecture images.", "",
              "Alpha limitations: AWS/Glue, Databricks, Snowflake and BigQuery live acceptance is blocked on credentials.",
              "Iceberg coverage and cross-source snapshot semantics remain bounded; see docs/LIMITATIONS.md.",
              "Authentication uses a service identity. This release does not add end-user delegation or native listener TLS.", "",
              "Installation and upgrades: docs/RELEASES.md. No public/latest tag is published.", ""]
    Path("release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    Path("release-notes.md").write_text("\n".join(notes))
    Path("SHA256SUMS").write_text(hashlib.sha256(Path("release-manifest.json").read_bytes()).hexdigest() + "  release-manifest.json\n")
    subprocess.run(["gh", "release", "create", "v" + version, "--target", commit,
                    "--title", "Neura " + version, "--prerelease", "--latest=false",
                    "--notes-file", "release-notes.md", "release-manifest.json", "SHA256SUMS"], check=True)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write("\n".join(notes))


if __name__ == "__main__":
    main()
