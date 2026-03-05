"""This is an example script for developing a simple pipeline for using
the BitTorrent protocol to create decentralised data repositories, focused in
archaeological data. The script is not intended to be used as-is, but rather as an
example of a possible workflow for creating a decentralised data repository using the BitTorrent protocol.
"""
import argparse
import os
import re
from datetime import datetime
import yaml
import uuid
import shutil
import hashlib
import libtorrent as lt

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "metadata_format.yaml")

def parse_args():
    parser = argparse.ArgumentParser(description="Validate and package a dataset directory.")
    parser.add_argument("-dir", "--dir", dest="dataset_dir", required=True, type=str, help="Dataset directory to process")
    parser.add_argument(
        "-schema",
        "--schema",
        dest="schema_file",
        default=SCHEMA_FILE,
        type=str,
        help="Schema file path (defaults to metadata_format.yaml)",
    )
    return parser.parse_args()

# Define the dataset directory from the command line argument
def define_dataset_directory(dataset_dir: str):
    if dataset_dir is None:
        raise ValueError("No directory provided. Please provide a directory using the -dir argument.")
    
    if not os.path.isdir(dataset_dir):
        raise ValueError(f"The provided directory '{dataset_dir}' does not exist or is not a directory.")
    return dataset_dir

# Define schema file from the command line argument
def define_schema_file_name(schema_file: str):
    if schema_file is None:
        schema_file = SCHEMA_FILE
    
    if not os.path.isfile(schema_file):
        raise ValueError(f"The provided schema file '{schema_file}' does not exist or is not a file.")
    return schema_file

# Dataset staticstics: Show the number of files in the dataset, the total size of the dataset, and the average file size.
# Does not include metadata file (metadata.yaml) in the statistics.

def get_dataset_statistics(dataset_dir):
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(dataset_dir):
        for file in files:
            if file != "metadata.yaml":
                file_count += 1
                total_size += os.path.getsize(os.path.join(root, file))
    average_file_size = total_size / file_count if file_count > 0 else 0
    return file_count, total_size, average_file_size

# Check format of the metadata file for each dataset.
# Fields and formats can be find in file metadata_format.yaml in the repository. 
# The script checks if the metadata file exists and if it contains the required fields with the correct formats.

def check_metadata_format(dataset_dir, schema_file):
    metadata_file = os.path.join(dataset_dir, "metadata.yaml")
    if not os.path.isfile(metadata_file):
        raise ValueError(f"Metadata file '{metadata_file}' does not exist.")
    with open(metadata_file, "r", encoding="utf-8") as f:
        metadata = yaml.safe_load(f)
    if not isinstance(metadata, dict):
        raise ValueError(f"Metadata file '{metadata_file}' must contain a YAML mapping at the top level.")

    with open(schema_file, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)
    schema_fields = schema.get("fields", {})

    errors = []

    def _add_error(field_name: str, message: str) -> None:
        errors.append(f"{field_name}: {message}")

    def _require(field_name: str):
        required = schema_fields.get(field_name, {}).get("required", False)
        if required and field_name not in metadata:
            _add_error(field_name, "field is missing")

    # Check required fields exist.
    for field in schema_fields:
        _require(field)

    # Validate primitive fields.
    id_value = metadata.get("id")
    if id_value is None or not isinstance(id_value, str) or not re.fullmatch(r"[a-z0-9-]+", id_value):
        _add_error("id", "must be a lowercase string containing digits and hyphens (e.g., jrdr-2026-002)")

    title_value = metadata.get("title")
    if not isinstance(title_value, str) or len(title_value.strip()) < 5:
        _add_error("title", "must be a string with at least 5 characters")

    version_value = metadata.get("version")
    if not isinstance(version_value, str) or not version_value.strip():
        _add_error("version", "must be a non-empty string (e.g., '1.0')")

    description_value = metadata.get("description")
    if not isinstance(description_value, str) or not description_value.strip():
        _add_error("description", "must be a non-empty string")

    authors_value = metadata.get("authors")
    if not isinstance(authors_value, list) or not authors_value:
        _add_error("authors", "must be a non-empty list")
    else:
        for idx, author in enumerate(authors_value):
            if not isinstance(author, dict) or "name" not in author or not isinstance(author["name"], str) or not author["name"].strip():
                _add_error(f"authors[{idx}]", "each author must be a mapping with a non-empty 'name' string")

    license_value = metadata.get("license")
    if not isinstance(license_value, str) or not license_value.strip():
        _add_error("license", "must be a non-empty string containing the SPDX identifier (e.g., CC-BY-4.0)")

    publication_date_value = metadata.get("publication_date")
    if not isinstance(publication_date_value, str):
        _add_error("publication_date", "must be a string in YYYY-MM-DD format")
    else:
        try:
            datetime.strptime(publication_date_value, "%Y-%m-%d")
        except (ValueError, TypeError):
            _add_error("publication_date", "must follow ISO 8601 YYYY-MM-DD format")

    language_value = metadata.get("language")
    if not isinstance(language_value, str) or len(language_value) != 2 or not language_value.islower():
        _add_error("language", "must be a lowercase ISO 639-1 code (e.g., 'en')")

    keywords_value = metadata.get("keywords")
    if not isinstance(keywords_value, list) or not keywords_value:
        _add_error("keywords", "must be a non-empty list of strings")
    else:
        for idx, keyword in enumerate(keywords_value):
            if not isinstance(keyword, str) or not keyword.strip():
                _add_error(f"keywords[{idx}]", "must be a non-empty string")

    related_publications_value = metadata.get("related_publications")
    if related_publications_value is not None:
        if not isinstance(related_publications_value, list):
            _add_error("related_publications", "must be a list when provided")
        else:
            for idx, entry in enumerate(related_publications_value):
                if not isinstance(entry, dict):
                    _add_error(f"related_publications[{idx}]", "must be a mapping")
                    continue
                if not isinstance(entry.get("title"), str) or not entry["title"].strip():
                    _add_error(f"related_publications[{idx}].title", "is required and must be a non-empty string")
                for optional_field in ("doi", "url", "conference"):
                    value = entry.get(optional_field)
                    if value is None:
                        continue
                    if not isinstance(value, str) or not value.strip():
                        _add_error(f"related_publications[{idx}].{optional_field}", "must be a non-empty string when provided")
                url_value = entry.get("url")
                if url_value and not url_value.startswith("https://"):
                    _add_error(f"related_publications[{idx}].url", "must start with https://")

    data_origin_value = metadata.get("data_origin")
    if not isinstance(data_origin_value, dict):
        _add_error("data_origin", "must be a mapping with source_project, field_season, location, and coordinate_reference_system")
    else:
        for subfield in ("source_project", "field_season", "location", "coordinate_reference_system"):
            val = data_origin_value.get(subfield)
            if not isinstance(val, str) or not val.strip():
                _add_error(f"data_origin.{subfield}", "is required and must be a non-empty string")

    files_value = metadata.get("files")
    if files_value is not None:
        if not isinstance(files_value, list):
            _add_error("files", "must be a list when provided")
        else:
            for idx, file_entry in enumerate(files_value):
                if not isinstance(file_entry, dict):
                    _add_error(f"files[{idx}]", "must be a mapping with 'path' and 'description' keys")
                    continue
                if not isinstance(file_entry.get("path"), str) or not file_entry["path"].strip():
                    _add_error(f"files[{idx}].path", "is required and must be a non-empty string")
                if not isinstance(file_entry.get("description"), str) or not file_entry["description"].strip():
                    _add_error(f"files[{idx}].description", "is required and must be a non-empty string")

    how_to_cite_value = metadata.get("how_to_cite")
    if not isinstance(how_to_cite_value, str) or not how_to_cite_value.strip():
        _add_error("how_to_cite", "must be a non-empty string")

    if errors:
        formatted = "\n- ".join(errors)
        raise ValueError(f"Metadata validation failed for '{metadata_file}':\n- {formatted}")

    print(f"Metadata file '{metadata_file}' passed validation against {SCHEMA_FILE}.")
    return metadata

# Create a zip file of the dataset, not including the metadata file, using an UUID as the name of the zip file. 
# The zip file will be created in the directory /temp (create it if it doesn't exist) as the dataset.

def create_zip_file(dataset_dir):
    temp_dir = os.path.join(os.getcwd(), "temp")
    os.makedirs(temp_dir, exist_ok=True)
    zip_uuid_str = str(uuid.uuid4())
    print(f"Zip file UUID: {zip_uuid_str}")
    zip_filename = f"{zip_uuid_str}.zip"
    zip_dir = os.path.join(temp_dir, zip_uuid_str)
    os.makedirs(zip_dir, exist_ok=True)
    zip_filepath = os.path.join(zip_dir, zip_filename)
    
    # Copy all files from the dataset directory to dir /files in the zip directory, except the metadata file.
    files_dir = os.path.join(zip_dir, "files")
    os.makedirs(files_dir, exist_ok=True)
    file_entries = []

    def _file_sha256(path, chunk_size=8192):
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                digest.update(chunk)
        return digest.hexdigest()

    for root, dirs, files in os.walk(dataset_dir):
        for n, file in enumerate(files, start=1):
            print(f"Processing file: {file}, {n} of {len(files)} in directory {root}")
            if file != "metadata.yaml":
                src_path = os.path.join(root, file)
                rel_path = os.path.relpath(src_path, dataset_dir)
                dst_path = os.path.join(files_dir, rel_path)
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                shutil.copy2(src_path, dst_path)
                size_bytes = os.path.getsize(dst_path)
                file_entries.append(
                    {
                        "rel_path": rel_path.replace(os.sep, "/"),
                        "size_bytes": size_bytes,
                        "sha256": _file_sha256(dst_path),
                    }
                )

    # Use the shutil module to create a zip file of the dataset, not including the metadata file, using an UUID as the name of the zip file.
    archive_base = os.path.splitext(zip_filepath)[0]
    print(f"Creating zip file at: {zip_filepath}")
    shutil.make_archive(base_name=archive_base, format="zip", root_dir=files_dir)
    
    
    
    
    # Update the metadata file in the zip directory to include the name of the zip file, the size of the zip file in bytes,
    # the date of creation of the zip file in ISO 8601 format and the hash of the zip file using the SHA256 algorithm.
    # Save the updated metadata file in the zip directory, with file name
    # zip_uuid_str_metadata.yaml (e.g., 123e4567-e89b-12d3-a456-426614174000_metadata.yaml).
    zip_metadata_file = os.path.join(zip_dir, f"{zip_uuid_str}_metadata.yaml")
    with open(os.path.join(dataset_dir, "metadata.yaml"), "r", encoding="utf-8") as f:
        metadata = yaml.safe_load(f)
    with open(zip_filepath, "rb") as f:
        zip_hash = hashlib.sha256(f.read()).hexdigest()
    metadata["manifests"] = {
        "files": {
            entry["rel_path"]: {
                "path": "/".join(["files", entry["rel_path"]]),
                "size_bytes": entry["size_bytes"],
                "sha256": entry["sha256"],
            }
            for entry in file_entries
        }
    }
    metadata["zip_file"] = {
        "uuid": zip_uuid_str,
        "size_bytes": os.path.getsize(zip_filepath),
        "creation_date": datetime.now().isoformat(),
        "sha256": zip_hash,
    }
    with open(zip_metadata_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(metadata, f, sort_keys=False)
    

    # Create a new zip file with the name of the dataset (id field in the metadata file) 
    # and the version (version field in the metadata file) in the format id-version.zip (e.g., jrdr-2026-002-1.0.zip), 
    # include the first zip file created in the previous step and the metadata file in the new zip file.
    dataset_id = metadata.get("id")
    dataset_version = metadata.get("version")
    global_zip_filename = f"{dataset_id}-{dataset_version}.zip"
    # Important: write the final archive outside the directory being archived to avoid self-inclusion. (Don't ask me how I know this.)
    global_zip_filepath = os.path.join(temp_dir, global_zip_filename)
    global_zip_base = os.path.splitext(global_zip_filepath)[0]
    
    shutil.make_archive(base_name=global_zip_base, format="zip", root_dir=zip_dir)
    print(f"Packaged dataset archive created at: {global_zip_filepath}")
    # Remove the temporary directory with the files, but keep the zip file and the metadata file in the zip directory.
    shutil.rmtree(files_dir)
    return global_zip_filepath, zip_filepath, zip_metadata_file, zip_uuid_str

# Create a torrent file from the zip file created in the previous step, using the libtorrent library. 
# The torrent file will be created in the same directory as the zip file, with the same name as the zip file but with the extension .torrent.
def create_torrent_file(zip_filepath):
    torrent_filename = os.path.splitext(zip_filepath)[0] + ".torrent"
    fs = lt.file_storage()
    lt.add_files(fs, zip_filepath)
    t = lt.create_torrent(fs)
    t.set_creator("Archaeological dataset Torrent Pipeline PoC")
    t.set_comment("Archaeological dataset Torrent Pipeline PoC script.")
    t.set_priv(False)  # Set to True if you want to create a private torrent (not distributed through DHT)
    
    # Load trackers from the torrent_config.yaml file and add them to the torrent.
    with open(os.path.join(os.path.dirname(__file__), "torrent_config.yaml"), "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    trackers = config.get("trackers", [])
    for tracker in trackers:
        t.add_tracker(tracker)

    lt.set_piece_hashes(t, os.path.dirname(zip_filepath))
    torrent_data = t.generate()
    
    with open(torrent_filename, "wb") as f:
        f.write(lt.bencode(torrent_data))
    
    print(f"Torrent file created at: {torrent_filename}")
    
    torrent_info = lt.torrent_info(torrent_filename)
    magnet_link = lt.make_magnet_uri(torrent_info)
    try:
        infohash_v1 = str(torrent_info.info_hashes().v1)
    except AttributeError:
        infohash_v1 = str(torrent_info.info_hash())
    print(f"Magnet link: {magnet_link}")
    
    return torrent_filename, magnet_link, infohash_v1

# Save in dir /final the zip file,  zip_metadata_fileand the torrent file created in the previous steps, 
# with the same name as the zip file but with the extension .zip and .torrent respectively.
def save_final_files(zip_metadata_file ,global_zip_filepath, torrent_filename, magnet_link, infohash_v1, zip_uuid_str):
    final_dir = os.path.join( os.path.dirname(__file__),"final", zip_uuid_str)
    os.makedirs(final_dir, exist_ok=True)
    
    with open(zip_metadata_file) as f:
        metadata = yaml.safe_load(f)
    torrent_info = lt.torrent_info(torrent_filename)
    tracker_urls = [tracker.url for tracker in torrent_info.trackers()]
    metadata["torrent"] = {
        "torrent_filename": os.path.basename(torrent_filename),
        "infohash_v1": infohash_v1,
        "magnet_link": magnet_link,
        "trackers": tracker_urls,
        "size_bytes": os.path.getsize(global_zip_filepath),
        "creation_date": datetime.now().isoformat()
    }
    
    final_metadata_file = os.path.join(final_dir, f"{zip_uuid_str}_metadata.yaml")
    with open(final_metadata_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(metadata, f, sort_keys=False)

    shutil.copy2(global_zip_filepath, final_dir)
    shutil.copy2(torrent_filename, final_dir)
    print(f"Final files saved in: {final_dir}")
    print(f"Zip file: {os.path.basename(global_zip_filepath)}")
    print(f"Metadata file: {os.path.basename(final_metadata_file)}")
    print(f"Torrent file: {os.path.basename(torrent_filename)}")

    print(f"Files saved in: {final_dir}")

# Delete all content on directory /temp: 
def cleanup_temporary_files(zip_filepath, zip_metadata_file):
    temp_dir = os.path.join(os.path.dirname(__file__), "temp")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"Temporary files in '{temp_dir}' have been cleaned up.")
    else:
        print(f"No temporary files found in '{temp_dir}' to clean up.")



if __name__ == "__main__":
    args = parse_args()
    dataset_dir = define_dataset_directory(args.dataset_dir)
    schema_file = define_schema_file_name(args.schema_file)
    file_count, total_size, average_file_size = get_dataset_statistics(dataset_dir)
    print(f"Number of files in the dataset: {file_count}")
    print(f"Total size of the dataset: {total_size} bytes")
    print(f"Average file size: {average_file_size} bytes")
    check_metadata_format(dataset_dir, schema_file)
    global_zip_filepath, zip_filepath, zip_metadata_file, zip_uuid_str = create_zip_file(dataset_dir)
    torrent_filename, magnet_link, infohash_v1 = create_torrent_file(global_zip_filepath)
    save_final_files(zip_metadata_file, global_zip_filepath, torrent_filename, magnet_link, infohash_v1, zip_uuid_str)
    cleanup_temporary_files(zip_filepath, zip_metadata_file)