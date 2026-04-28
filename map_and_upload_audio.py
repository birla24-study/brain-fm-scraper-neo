#!/usr/bin/env python3
"""Map scraped JSON tracks to local MP3 files and upload them to Appwrite Storage.

Usage:
  export APPWRITE_ENDPOINT="https://fra.cloud.appwrite.io/v1"
  export APPWRITE_PROJECT_ID="mind-fm"
  export APPWRITE_API_KEY="..."
  python3 map_and_upload_audio.py --bucket audio --workers 8
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import unquote, urlparse


SONG_RE = re.compile(r"^(?P<title>.+) \((?P<genre>.+)\)$")
FIXED_ID_LEN = 20


@dataclass
class TrackRef:
    source_json: str
    index: int
    data: dict


@dataclass
class MappedTrack:
    track: TrackRef
    file_path: Path
    map_method: str


def normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def safe_str(value: object, default: str = "Unknown") -> str:
    if value is None:
        return default
    s = str(value).strip()
    return s if s else default


def valid_appwrite_id(value: str) -> bool:
    if not value:
        return False
    if len(value) > 36:
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9._-]+", value))


def valid_fixed_id(value: str, length: int) -> bool:
    return len(value) == length and bool(re.fullmatch(r"[a-f0-9]+", value))


def build_track_seed(track: dict) -> str:
    parts = [
        basename_from_url(track),
        safe_str(track.get("song_name")),
        safe_str(track.get("genre")),
        safe_str(track.get("activity")),
        safe_str(track.get("sub_activity")),
        safe_str(track.get("complexity")),
    ]
    return "|".join(parts)


def make_fixed_id(seed: str, length: int) -> str:
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:length]


def build_id_map(tracks: List[TrackRef], id_length: int) -> Dict[Tuple[str, int], str]:
    assigned: Dict[str, Tuple[str, int]] = {}
    out: Dict[Tuple[str, int], str] = {}

    for t in tracks:
        key = (t.source_json, t.index)
        existing = safe_str(t.data.get("$id"), "")
        if valid_fixed_id(existing, id_length):
            candidate = existing
        else:
            candidate = make_fixed_id(build_track_seed(t.data), id_length)

        suffix = 0
        base = candidate
        while candidate in assigned and assigned[candidate] != key:
            suffix += 1
            candidate = make_fixed_id(f"{base}|{suffix}", id_length)

        assigned[candidate] = key
        out[key] = candidate

    return out


def write_ids_into_json(output_dir: Path, tracks: List[TrackRef], id_map: Dict[Tuple[str, int], str]) -> int:
    changed_files = 0
    by_file: Dict[str, List[TrackRef]] = {}
    for t in tracks:
        by_file.setdefault(t.source_json, []).append(t)

    for source_json, refs in by_file.items():
        file_path = output_dir / source_json
        with file_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)

        if not isinstance(payload, list):
            continue

        file_changed = False
        for ref in refs:
            if ref.index >= len(payload):
                continue
            obj = payload[ref.index]
            if not isinstance(obj, dict):
                continue

            wanted_id = id_map[(ref.source_json, ref.index)]
            if obj.get("$id") == wanted_id:
                continue

            new_obj = {"$id": wanted_id}
            for k, v in obj.items():
                if k == "$id":
                    continue
                new_obj[k] = v

            payload[ref.index] = new_obj
            file_changed = True

        if file_changed:
            with file_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
                fh.write("\n")
            changed_files += 1

    return changed_files


def expected_download_rel(track: dict) -> Path:
    activity = safe_str(track.get("activity"))
    sub_activity = safe_str(track.get("sub_activity"))
    complexity = safe_str(track.get("complexity"))
    genre = safe_str(track.get("genre"))
    song_name = safe_str(track.get("song_name"))

    album_dir = f"{activity}: {sub_activity} - {complexity}"
    filename = f"{song_name} ({genre}).mp3"
    return Path(album_dir) / genre / filename


def basename_from_url(track: dict) -> str:
    url = safe_str(track.get("url"), "")
    if not url:
        return ""
    parsed = urlparse(url)
    return unquote(Path(parsed.path).name)


def extract_title_and_genre(file_name: str) -> Tuple[str, str]:
    stem = Path(file_name).stem
    m = SONG_RE.match(stem)
    if m:
        return m.group("title"), m.group("genre")
    return stem, ""


def build_download_index(downloads_root: Path) -> dict:
    by_relative: Dict[str, Path] = {}
    by_basename: Dict[str, List[Path]] = {}
    by_album_genre_title: Dict[Tuple[str, str, str], List[Path]] = {}

    for file_path in downloads_root.rglob("*.mp3"):
        rel = file_path.relative_to(downloads_root).as_posix()
        by_relative[rel] = file_path

        basename = file_path.name
        by_basename.setdefault(normalize(basename), []).append(file_path)

        parts = file_path.parts
        if len(parts) < 3:
            continue

        album = parts[-3]
        genre_dir = parts[-2]
        title, _ = extract_title_and_genre(basename)
        key = (normalize(album), normalize(genre_dir), normalize(title))
        by_album_genre_title.setdefault(key, []).append(file_path)

    return {
        "by_relative": by_relative,
        "by_basename": by_basename,
        "by_album_genre_title": by_album_genre_title,
    }


def collect_tracks(output_dir: Path) -> List[TrackRef]:
    tracks: List[TrackRef] = []
    for json_file in sorted(output_dir.glob("*.json")):
        with json_file.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, list):
            continue
        for idx, item in enumerate(payload):
            if isinstance(item, dict):
                tracks.append(TrackRef(source_json=json_file.name, index=idx, data=item))
    return tracks


def collect_tracks_from_files(output_dir: Path, selected_json_files: List[str]) -> List[TrackRef]:
    tracks: List[TrackRef] = []
    if not selected_json_files:
        return collect_tracks(output_dir)

    for name in selected_json_files:
        json_file = output_dir / name
        if not json_file.exists():
            raise FileNotFoundError(f"Selected JSON file not found in output dir: {name}")

        with json_file.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, list):
            continue
        for idx, item in enumerate(payload):
            if isinstance(item, dict):
                tracks.append(TrackRef(source_json=json_file.name, index=idx, data=item))
    return tracks


def load_upload_tracker(path: Path) -> dict:
    if not path.exists():
        return {"uploaded_ids": {}, "updated_at": ""}
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        return {"uploaded_ids": {}, "updated_at": ""}
    payload.setdefault("uploaded_ids", {})
    if not isinstance(payload["uploaded_ids"], dict):
        payload["uploaded_ids"] = {}
    payload.setdefault("updated_at", "")
    return payload


def save_upload_tracker(path: Path, tracker: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(tracker, fh, indent=2)
        fh.write("\n")


def track_key(track_ref: TrackRef, track_id: str) -> str:
    return f"{track_ref.source_json}#{track_ref.index}#{track_id}"


def map_tracks(tracks: List[TrackRef], downloads_root: Path, index: dict) -> Tuple[List[MappedTrack], List[dict]]:
    mapped: List[MappedTrack] = []
    unmatched: List[dict] = []

    by_relative: Dict[str, Path] = index["by_relative"]
    by_basename: Dict[str, List[Path]] = index["by_basename"]
    by_album_genre_title: Dict[Tuple[str, str, str], List[Path]] = index["by_album_genre_title"]

    for t in tracks:
        track = t.data

        rel = expected_download_rel(track).as_posix()
        direct = by_relative.get(rel)
        if direct:
            mapped.append(MappedTrack(track=t, file_path=direct, map_method="direct-path"))
            continue

        url_basename = basename_from_url(track)
        if url_basename:
            candidates = by_basename.get(normalize(url_basename), [])
            if len(candidates) == 1:
                mapped.append(MappedTrack(track=t, file_path=candidates[0], map_method="url-basename"))
                continue

        activity = safe_str(track.get("activity"))
        sub_activity = safe_str(track.get("sub_activity"))
        complexity = safe_str(track.get("complexity"))
        genre = safe_str(track.get("genre"))
        song_name = safe_str(track.get("song_name"))
        album = f"{activity}: {sub_activity} - {complexity}"
        key = (normalize(album), normalize(genre), normalize(song_name))

        candidates = by_album_genre_title.get(key, [])
        if len(candidates) == 1:
            mapped.append(MappedTrack(track=t, file_path=candidates[0], map_method="album-genre-title"))
            continue

        unmatched.append(
            {
                "source_json": t.source_json,
                "index": t.index,
                "song_name": song_name,
                "genre": genre,
                "expected_relative": rel,
                "url_basename": url_basename,
                "candidate_count": len(candidates),
            }
        )

    return mapped, unmatched


def deterministic_file_id(downloads_root: Path, file_path: Path) -> str:
    rel = file_path.relative_to(downloads_root).as_posix()
    return hashlib.sha1(rel.encode("utf-8")).hexdigest()[:32]


def upload_one(storage: Any, bucket_id: str, downloads_root: Path, mapped: MappedTrack, id_length: int) -> dict:
    from appwrite.input_file import InputFile

    track = mapped.track.data
    json_id = safe_str(track.get("$id"), "")
    if not valid_fixed_id(json_id, id_length):
        raise ValueError(
            f"Missing or invalid fixed $id in {mapped.track.source_json} at index {mapped.track.index}: {json_id}"
        )
    file_id = json_id
    rel = mapped.file_path.relative_to(downloads_root).as_posix()

    try:
        resp = storage.create_file(
            bucket_id=bucket_id,
            file_id=file_id,
            file=InputFile.from_path(str(mapped.file_path)),
        )
        return {
            "status": "uploaded",
            "file_id": resp.get("$id", file_id),
            "relative_path": rel,
            "map_method": mapped.map_method,
            "source_json": mapped.track.source_json,
            "track_index": mapped.track.index,
            "song_name": safe_str(track.get("song_name")),
            "song_id": file_id,
        }
    except Exception as exc:
        msg = str(exc)
        if "already exists" in msg.lower() or "409" in msg:
            return {
                "status": "skipped-exists",
                "file_id": file_id,
                "relative_path": rel,
                "map_method": mapped.map_method,
                "source_json": mapped.track.source_json,
                "track_index": mapped.track.index,
                "song_name": safe_str(track.get("song_name")),
                "song_id": file_id,
            }
        return {
            "status": "failed",
            "file_id": file_id,
            "relative_path": rel,
            "map_method": mapped.map_method,
            "source_json": mapped.track.source_json,
            "track_index": mapped.track.index,
            "song_name": safe_str(track.get("song_name")),
            "song_id": file_id,
            "error": msg,
        }


def make_client() -> Any:
    from appwrite.client import Client

    endpoint = os.getenv("APPWRITE_ENDPOINT")
    project = os.getenv("APPWRITE_PROJECT_ID")
    api_key = os.getenv("APPWRITE_API_KEY")

    missing = [name for name, value in {
        "APPWRITE_ENDPOINT": endpoint,
        "APPWRITE_PROJECT_ID": project,
        "APPWRITE_API_KEY": api_key,
    }.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return Client().set_endpoint(endpoint).set_project(project).set_key(api_key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map JSON tracks to local files and upload to Appwrite.")
    parser.add_argument("--output-dir", default="output", help="Directory containing scraped JSON files")
    parser.add_argument("--downloads-dir", default="downloads", help="Directory containing downloaded MP3 files")
    parser.add_argument("--bucket", default="audio", help="Target Appwrite bucket ID")
    parser.add_argument("--workers", type=int, default=6, help="Concurrent upload workers")
    parser.add_argument("--manifest", default="output/upload_manifest_audio.json", help="Manifest output path")
    parser.add_argument("--id-length", type=int, default=FIXED_ID_LEN, help="Fixed length for generated $id")
    parser.add_argument("--write-ids", action="store_true", help="Embed generated $id in JSON files before mapping")
    parser.add_argument(
        "--json-file",
        action="append",
        default=[],
        help="Upload only this JSON file from output dir (repeatable), e.g. --json-file 'Focus - Motivation.json'",
    )
    parser.add_argument(
        "--tracker",
        default="output/uploaded_audio_tracker.json",
        help="Path to persistent upload tracker JSON",
    )
    parser.add_argument(
        "--skip-tracked",
        action="store_true",
        help="Skip files already marked uploaded in tracker",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only map files, do not upload")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    downloads_dir = Path(args.downloads_dir).resolve()
    manifest_path = Path(args.manifest).resolve()

    if not output_dir.exists():
        raise FileNotFoundError(f"Output directory not found: {output_dir}")
    if not downloads_dir.exists():
        raise FileNotFoundError(f"Downloads directory not found: {downloads_dir}")

    tracks = collect_tracks_from_files(output_dir, args.json_file)
    id_map = build_id_map(tracks, args.id_length)

    json_files_updated = 0
    if args.write_ids:
        json_files_updated = write_ids_into_json(output_dir, tracks, id_map)
        tracks = collect_tracks_from_files(output_dir, args.json_file)

    tracks_with_json_id = sum(
        1 for t in tracks if valid_fixed_id(safe_str(t.data.get("$id"), ""), args.id_length)
    )
    tracks_missing_json_id = len(tracks) - tracks_with_json_id
    index = build_download_index(downloads_dir)
    mapped, unmatched = map_tracks(tracks, downloads_dir, index)

    summary = {
        "total_tracks": len(tracks),
        "mapped": len(mapped),
        "unmatched": len(unmatched),
        "tracks_with_json_id": tracks_with_json_id,
        "tracks_missing_json_id": tracks_missing_json_id,
        "json_files_updated": json_files_updated,
        "id_length": args.id_length,
        "selected_json_files": args.json_file,
        "dry_run": bool(args.dry_run),
        "bucket": args.bucket,
    }

    print(json.dumps(summary, indent=2))

    uploads: List[dict] = []
    if not args.dry_run:
        from appwrite.services.storage import Storage

        client = make_client()
        storage = Storage(client)
        tracker_path = Path(args.tracker).resolve()
        tracker = load_upload_tracker(tracker_path)
        uploaded_ids = tracker.get("uploaded_ids", {})

        upload_candidates = mapped
        skipped_tracked_count = 0
        if args.skip_tracked:
            filtered: List[MappedTrack] = []
            for m in mapped:
                track = m.track.data
                track_id = safe_str(track.get("$id"), "")
                key = track_key(m.track, track_id)
                if key in uploaded_ids:
                    skipped_tracked_count += 1
                    uploads.append(
                        {
                            "status": "skipped-tracked",
                            "file_id": track_id,
                            "relative_path": m.file_path.relative_to(downloads_dir).as_posix(),
                            "map_method": m.map_method,
                            "source_json": m.track.source_json,
                            "track_index": m.track.index,
                            "song_name": safe_str(track.get("song_name")),
                            "song_id": track_id,
                        }
                    )
                else:
                    filtered.append(m)
            upload_candidates = filtered

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [
                pool.submit(upload_one, storage, args.bucket, downloads_dir, m, args.id_length)
                for m in upload_candidates
            ]
            for fut in concurrent.futures.as_completed(futures):
                result = fut.result()
                uploads.append(result)

                if result["status"] in {"uploaded", "skipped-exists"}:
                    key = track_key(
                        TrackRef(source_json=result["source_json"], index=int(result["track_index"]), data={}),
                        safe_str(result.get("song_id"), ""),
                    )
                    uploaded_ids[key] = {
                        "file_id": result.get("file_id"),
                        "relative_path": result.get("relative_path"),
                        "source_json": result.get("source_json"),
                        "song_name": result.get("song_name"),
                        "status": result.get("status"),
                    }

        from datetime import datetime, timezone

        tracker["uploaded_ids"] = uploaded_ids
        tracker["updated_at"] = datetime.now(timezone.utc).isoformat()
        save_upload_tracker(tracker_path, tracker)

        uploaded = sum(1 for x in uploads if x["status"] == "uploaded")
        skipped = sum(1 for x in uploads if x["status"] == "skipped-exists")
        skipped_tracked = sum(1 for x in uploads if x["status"] == "skipped-tracked")
        failed = sum(1 for x in uploads if x["status"] == "failed")
        print(
            f"Uploaded: {uploaded}, Skipped(existing): {skipped}, "
            f"Skipped(tracked): {skipped_tracked}, Failed: {failed}"
        )

    manifest = {
        "summary": summary,
        "unmatched": unmatched,
        "uploads": uploads,
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    print(f"Manifest written to: {manifest_path}")


if __name__ == "__main__":
    main()
