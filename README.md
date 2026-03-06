# CAA Torrent Pipeline

Proof-of-concept workflow for packaging archaeological datasets into signed ZIP archives and BitTorrent releases. Built for the 2026 CAA conference in Vienna, it validates dataset metadata, builds reproducible archives, and emits `.torrent` files so datasets can be shared peer-to-peer.

## What it does
- Takes a dataset folder containing a `metadata.yaml` and associated files. This script considers all files in the folder except `metadata.yaml` as part of the dataset to be packaged.
- Validates `metadata.yaml` against the schema in `metadata_format.yaml` (required fields, formats, and basic types).
- Reports dataset stats (file count, total size, average size) excluding the metadata file.
- Packages dataset files (excluding `metadata.yaml`) under a UUID named temporal directory, computing SHA-256 checksums and writing a manifest.
- Produces a release ZIP named `<id>-<version>.zip` plus a matching `.torrent`, using trackers from `torrent_config.yaml`, and prints the magnet link.
- Writes enriched metadata (manifests, ZIP info, torrent info) alongside the artifacts in `final/<uuid>/`, then cleans the temporary directory.

## Repository layout
- `dataset_torrent_pipeline.py` — Main script, CLI pipeline orchestrating validation, packaging, torrent generation, and cleanup.
- `metadata_format.yaml` — schema describing required metadata fields for datasets.
- `torrent_config.yaml` — Details for torrent creation (trackers, piece size).
- `pyproject.toml` — Python project configuration, including dependencies and metadata.


## Requirements
- Python 3.12+
- Dependencies: libtorrent, libtorrent-windows-dll, pandas, pyyaml (managed via `pyproject.toml`)
- Tested on Windows; Linux/macOS should work with libtorrent installed.

## Quick Start With uv
If `uv` is not installed yet, install it first:
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Then clone and run:
```powershell
git clone <YOUR_REPOSITORY_URL>
cd CAA_torrent

# Create virtual environment and install dependencies from pyproject.toml
uv sync

# Run with your own dataset folder
uv run .\dataset_torrent_pipeline.py -dir .\path\to\your_dataset\

# Run with your own dataset and custom metadata schema
uv run .\dataset_torrent_pipeline.py -dir .\path\to\your_dataset\ -schema .\path\to\your_dataset\metadata_format.yaml
```


## Usage
```bash
uv run .\dataset_torrent_pipeline.py -dir path/to/dataset [-schema path/to/custom_schema.yaml]
```
- `-dir` (required): dataset folder containing `metadata.yaml` plus the files to package.
- `-schema` (optional): override the default schema file if you maintain your own.

## Metadata expectations (summary)
- `id`: lowercase string with digits/hyphens (e.g., `jrdr-2026-002`).
- `title`: string, length >= 5.
- `version`: non-empty string (e.g., `1.0`).
- `description`: non-empty string (multi-line allowed).
- `authors`: list of mappings with `name`.
- `license`: SPDX identifier (e.g., `CC-BY-4.0`).
- `publication_date`: `YYYY-MM-DD` (ISO 8601).
- `language`: ISO 639-1 code (e.g., `en`).
- `keywords`: non-empty list of strings.
- `data_origin`: mapping with `source_project`, `field_season`, `location`, `coordinate_reference_system`.
- Optional: `related_publications` (list of title/doi/url/conference), `files` (list of path/description), `how_to_cite` (string, required).

## Outputs
- `final/<uuid>/<id>-<version>.zip` — release archive containing the packaged dataset bundle.
- `final/<uuid>/<uuid>_metadata.yaml` — original metadata enriched with manifests, ZIP info, torrent info, and tracker list.
- `final/<uuid>/<id>-<version>.torrent` — torrent file; the magnet link is printed to stdout.
- Temporary workspace lives under `temp/` and is removed after a run.

## Notes
- Trackers come from `torrent_config.yaml`; adjust to your infrastructure if needed.
- The pipeline excludes `metadata.yaml` from the packaged files but embeds it (with manifests) in the release bundle.

## Author
Created by Javier F. Palomeque —  https://jfpalomeque.com
