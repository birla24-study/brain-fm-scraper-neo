import os
import json
import subprocess
import shutil
import re
import argparse
from concurrent.futures import ThreadPoolExecutor
import urllib.request
import urllib.parse
from PIL import Image

def get_json_files(directory):
    if not os.path.exists(directory):
        return []
    return [f for f in os.listdir(directory) if f.endswith('.json')]

def get_safe_filename(name):
    return "".join([c for c in name if c.isalpha() or c.isdigit() or c in ' -_()']).rstrip()

def extract_neural_effect(effect_str):
    if not effect_str:
        return "Unknown"
    return re.sub(r'(?i)\s*neural effect', '', effect_str).strip()

# --- PHASE 1: BATCH DOWNLOAD ---

def download_single_audio(source_url, temp_raw_path):
    if not source_url or os.path.exists(temp_raw_path):
        return os.path.exists(temp_raw_path)
    os.makedirs(os.path.dirname(temp_raw_path), exist_ok=True)
    try:
        subprocess.run(
            [
                "aria2c",
                "-x16",
                "-s16",
                "-j16",
                "--allow-overwrite=true",
                "--auto-file-renaming=false",
                "--dir", os.path.dirname(temp_raw_path),
                "--out", os.path.basename(temp_raw_path),
                source_url,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True
    except Exception as e:
        print(f"Error downloading audio to {temp_raw_path}: {e}", flush=True)
        if os.path.exists(temp_raw_path):
            try:
                os.remove(temp_raw_path)
            except:
                pass
        return False

def download_single_cover(cover_url, temp_raw_cover_path):
    if not cover_url or os.path.exists(temp_raw_cover_path):
        return os.path.exists(temp_raw_cover_path)
    os.makedirs(os.path.dirname(temp_raw_cover_path), exist_ok=True)
    try:
        parsed = urllib.parse.urlparse(cover_url)
        query_params = urllib.parse.parse_qsl(parsed.query)
        new_params = []
        has_w = False
        for k, v in query_params:
            if k == 'q':
                continue
            if k == 'w':
                v = '2400'
                has_w = True
            new_params.append((k, v))
        if not has_w:
            new_params.append(('w', '2400'))
        modified_url = urllib.parse.urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            parsed.params, urllib.parse.urlencode(new_params), parsed.fragment
        ))
        req = urllib.request.Request(
            modified_url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            with open(temp_raw_cover_path, 'wb') as f:
                f.write(response.read())
        return True
    except Exception as e:
        if os.path.exists(temp_raw_cover_path):
            try:
                os.remove(temp_raw_cover_path)
            except:
                pass
        return False

def download_track_resources(item, track_idx, total_tracks):
    final_filepath = item["final_filepath"]
    if os.path.exists(final_filepath):
        print(f"[{track_idx}/{total_tracks}] Download skipped (already finished): {item['song_name']}", flush=True)
        return

    track = item["track"]
    print(f"[{track_idx}/{total_tracks}] Downloading resources for: {item['song_name']}", flush=True)
    
    temp_raw_audio = final_filepath + ".raw.mp3"
    temp_raw_cover = final_filepath + ".raw_cover.jpg"

    download_single_audio(track.get("url"), temp_raw_audio)
    download_single_cover(track.get("cover_url"), temp_raw_cover)

# --- PHASE 2: BATCH CROP & RESIZE ---

def crop_and_resize_single_cover(temp_raw_cover_path, temp_processed_cover_path, cover_size):
    if not os.path.exists(temp_raw_cover_path):
        return None
    try:
        with Image.open(temp_raw_cover_path) as img:
            if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                rgba_img = img.convert('RGBA')
                background = Image.new('RGB', rgba_img.size, (255, 255, 255))
                background.paste(rgba_img, mask=rgba_img.split()[3])
                img = background
            else:
                img = img.convert('RGB')
                
            width, height = img.size
            min_dim = min(width, height)
            left = (width - min_dim) / 2
            top = (height - min_dim) / 2
            right = (width + min_dim) / 2
            bottom = (height + min_dim) / 2
            
            cropped = img.crop((left, top, right, bottom))
            resized = cropped.resize((cover_size, cover_size), Image.Resampling.LANCZOS)
            resized.save(temp_processed_cover_path)
        os.remove(temp_raw_cover_path)
        return temp_processed_cover_path
    except Exception as e:
        print(f"Error cropping cover {temp_raw_cover_path}: {e}", flush=True)
        return None

def process_track_cover(item, cover_size, track_idx, total_tracks):
    final_filepath = item["final_filepath"]
    temp_raw_cover = final_filepath + ".raw_cover.jpg"
    temp_processed_cover = final_filepath + ".cover.jpg"
    
    if os.path.exists(temp_raw_cover):
        print(f"[{track_idx}/{total_tracks}] Cropping cover for: {item['song_name']}", flush=True)
        crop_and_resize_single_cover(temp_raw_cover, temp_processed_cover, cover_size)

# --- PHASE 3: BATCH TAG & OUTPUT ---

def tag_and_output_single_track(item, track_idx, total_tracks):
    final_filepath = item["final_filepath"]
    if os.path.exists(final_filepath):
        return

    temp_raw_audio = final_filepath + ".raw.mp3"
    temp_processed_cover = final_filepath + ".cover.jpg"

    if not os.path.exists(temp_raw_audio):
        print(f"[{track_idx}/{total_tracks}] Skipping tagging (no raw audio): {item['song_name']}", flush=True)
        return

    print(f"[{track_idx}/{total_tracks}] Tagging & Exporting: {item['song_name']}", flush=True)

    track = item["track"]
    title = item["title"]
    album = item["album"]
    tmp_output = final_filepath + ".tmp.mp3"

    cmd = ["ffmpeg", "-y", "-i", temp_raw_audio]
    has_cover = os.path.exists(temp_processed_cover)
    if has_cover:
        cmd.extend(["-i", temp_processed_cover, "-map", "0:a:0", "-map", "1:0"])
    else:
        cmd.extend(["-map", "0:a:0"])

    cmd.extend([
        "-c", "copy",
        "-id3v2_version", "3",
        "-metadata", f"title={title}",
        "-metadata", f"album={album}",
        "-metadata", "artist=BrainFM",
        "-metadata", "album_artist=BrainFM",
        "-metadata", f"genre={track.get('genre', 'Unknown')}"
    ])

    schema_fields = [
        "neural_effect", "activity", "sub_activity", "genre",
        "moods", "instrumentation", "complexity", "brightness"
    ]
    for field in schema_fields:
        val = track.get(field)
        if val is not None and val != "":
            cmd.extend(["-metadata", f"{field}={val}"])

    comment_parts = []
    if track.get("genre"): comment_parts.append(f"Genre: {track.get('genre')}")
    if track.get("moods"): comment_parts.append(f"Moods: {track.get('moods')}")
    if track.get("instrumentation"): comment_parts.append(f"Instrumentation: {track.get('instrumentation')}")
    if track.get("complexity"): comment_parts.append(f"Complexity: {track.get('complexity')}")
    if track.get("brightness"): comment_parts.append(f"Brightness: {track.get('brightness')}")
    comment_str = " | ".join(comment_parts)
    if comment_str:
        cmd.extend(["-metadata", f"comment={comment_str}"])

    if has_cover:
        cmd.extend([
            "-metadata:s:v", "title=Album cover",
            "-metadata:s:v", "comment=Cover (front)",
            "-disposition:v", "attached_pic"
        ])

    cmd.append(tmp_output)

    try:
        os.makedirs(os.path.dirname(final_filepath), exist_ok=True)
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        shutil.move(tmp_output, final_filepath)
        print(f"[{track_idx}/{total_tracks}] Completed: {item['song_name']}", flush=True)
    except Exception as e:
        print(f"Error tagging {item['song_name']}: {e}", flush=True)
        if os.path.exists(tmp_output):
            try:
                os.remove(tmp_output)
            except:
                pass
    finally:
        for tmp_f in [temp_raw_audio, temp_processed_cover]:
            if os.path.exists(tmp_f):
                try:
                    os.remove(tmp_f)
                except:
                    pass

def main():
    parser = argparse.ArgumentParser(description="Download, crop, and tag BrainFM tracks in a 3-Phase Batch Pipeline directly to output directory.")
    parser.add_argument("-i", "--input", help="Input directory containing JSON files (default: input).")
    parser.add_argument("-o", "--output", help="Destination folder for tagged files (default: output).")
    parser.add_argument("-a", "--activity", help="Filter by activity (e.g., Focus). Case-insensitive.")
    parser.add_argument("-g", "--genre", help="Filter by genre (e.g., LoFi). Case-insensitive.")
    parser.add_argument("-n", "--neural", help="Filter by neural level (e.g., High, Medium, Low). Case-insensitive.")
    parser.add_argument("-s", "--size", type=int, default=1080, help="Size of the cover art (width and height in pixels). Default is 1080.")
    parser.add_argument("-t", "--threads", type=int, default=16, help="Number of concurrent worker threads. Default is 16.")
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.dirname(__file__))
    input_dir = os.path.abspath(args.input) if args.input else os.path.join(base_dir, "input")

    if not os.path.exists(input_dir):
        os.makedirs(input_dir, exist_ok=True)

    json_files = get_json_files(input_dir)
    if not json_files:
        print(f"No JSON metadata files found in '{input_dir}'. Please place JSON metadata files there.")
        return

    tracks = []
    for jf in json_files:
        with open(os.path.join(input_dir, jf), 'r', encoding='utf-8') as f:
            tracks.extend(json.load(f))

    if not tracks:
        print("No tracks found in the input JSON metadata files.")
        return

    # Build track processing list
    all_tracks_info = []
    for track in tracks:
        activity = track.get("activity", "Unknown")
        sub_activity = track.get("sub_activity", "Unknown")
        genre = track.get("genre", "Unknown")
        neural_effect_full = track.get("neural_effect", "Unknown")
        neural_effect = extract_neural_effect(neural_effect_full)
        song_name = track.get("song_name", "Unknown")

        album_folder = f"{activity}:{sub_activity} ({neural_effect})"
        title = f"{song_name} ({neural_effect})"
        safe_title = get_safe_filename(title)
        album = f"{activity}: {sub_activity}"

        all_tracks_info.append({
            "track": track,
            "activity": activity,
            "sub_activity": sub_activity,
            "neural_effect": neural_effect,
            "song_name": song_name,
            "genre": genre,
            "album_folder": album_folder,
            "safe_title": safe_title,
            "album": album,
            "title": title
        })

    is_interactive = args.output is None

    if is_interactive:
        print("=== Brain.fm Downloader (3-Phase Batch Pipeline) ===")
        target_dir_input = input("Enter destination folder [default: output]: ").strip()
        if not target_dir_input:
            target_dir_input = "output"
        target_dir = os.path.abspath(target_dir_input)

        print("\nSelect cover art size optimization:")
        print("1. Phone / Tablet (1080x1080)")
        print("2. Standard MP3 Player (600x600)")
        print("3. Compact MP3 Player / Legacy (300x300)")
        print("4. Custom size")

        size_choice = ""
        while size_choice not in ["1", "2", "3", "4"]:
            size_choice = input("Select option (1-4) [default: 1]: ").strip()
            if not size_choice:
                size_choice = "1"

        if size_choice == "1":
            cover_size = 1080
        elif size_choice == "2":
            cover_size = 600
        elif size_choice == "3":
            cover_size = 300
        elif size_choice == "4":
            while True:
                try:
                    custom_size_input = input("Enter custom size [default: 1080]: ").strip()
                    if not custom_size_input:
                        cover_size = 1080
                        break
                    cover_size = int(custom_size_input)
                    if cover_size > 0:
                        break
                    print("Size must be a positive integer.")
                except ValueError:
                    print("Invalid input.")

        print(f"\nFound {len(all_tracks_info)} tracks in JSON metadata.\n")
        print("Choose processing mode:")
        print("1. Download & tag all tracks")
        print("2. Filter and select specific tracks")

        mode = ""
        while mode not in ["1", "2"]:
            mode = input("Select option (1-2) [default: 1]: ").strip()
            if not mode:
                mode = "1"

        candidate_tracks = all_tracks_info

        if mode == "2":
            # Filter Activities
            activities = sorted(list(set(t["activity"] for t in candidate_tracks)))
            print("\nAvailable Activities:")
            print("0. All Activities")
            for idx, act in enumerate(activities, 1):
                print(f"{idx}. {act}")

            act_choice = -1
            while act_choice < 0 or act_choice > len(activities):
                try:
                    act_choice = int(input(f"Select Activity (0-{len(activities)}): ").strip())
                except ValueError:
                    pass
            if act_choice > 0:
                candidate_tracks = [t for t in candidate_tracks if t["activity"] == activities[act_choice - 1]]

            # Filter Sub-activities
            sub_activities = sorted(list(set(t["sub_activity"] for t in candidate_tracks)))
            print("\nAvailable Sub-activities:")
            print("0. All Sub-activities")
            for idx, sub in enumerate(sub_activities, 1):
                print(f"{idx}. {sub}")

            sub_choice = -1
            while sub_choice < 0 or sub_choice > len(sub_activities):
                try:
                    sub_choice = int(input(f"Select Sub-activity (0-{len(sub_activities)}): ").strip())
                except ValueError:
                    pass
            if sub_choice > 0:
                candidate_tracks = [t for t in candidate_tracks if t["sub_activity"] == sub_activities[sub_choice - 1]]

            # Filter Genres
            genres = sorted(list(set(t["genre"] for t in candidate_tracks)))
            print("\nAvailable Genres:")
            print("0. All Genres")
            for idx, gen in enumerate(genres, 1):
                print(f"{idx}. {gen}")

            gen_choice = -1
            while gen_choice < 0 or gen_choice > len(genres):
                try:
                    gen_choice = int(input(f"Select Genre (0-{len(genres)}): ").strip())
                except ValueError:
                    pass
            if gen_choice > 0:
                candidate_tracks = [t for t in candidate_tracks if t["genre"] == genres[gen_choice - 1]]

            # Filter Neural Levels
            neural_levels = sorted(list(set(t["neural_effect"] for t in candidate_tracks)))
            print("\nAvailable Neural Levels:")
            print("0. All Neural Levels")
            for idx, nl in enumerate(neural_levels, 1):
                print(f"{idx}. {nl}")

            nl_choice = -1
            while nl_choice < 0 or nl_choice > len(neural_levels):
                try:
                    nl_choice = int(input(f"Select Neural Level (0-{len(neural_levels)}): ").strip())
                except ValueError:
                    pass
            if nl_choice > 0:
                candidate_tracks = [t for t in candidate_tracks if t["neural_effect"] == neural_levels[nl_choice - 1]]

        selected_tracks = candidate_tracks

        if not selected_tracks:
            print("No tracks matched your criteria.")
            return

        confirm = input(f"\nProceed with processing {len(selected_tracks)} tracks? (y/n) [default: y]: ").strip().lower()
        if confirm == 'n':
            print("Aborted.")
            return

    else:
        target_dir = os.path.abspath(args.output)
        cover_size = args.size
        selected_tracks = []
        for t in all_tracks_info:
            if args.activity and args.activity.lower() != t["activity"].lower():
                continue
            if args.genre and args.genre.lower() != t["genre"].lower():
                continue
            if args.neural and args.neural.lower() != t["neural_effect"].lower():
                continue
            selected_tracks.append(t)

        if not selected_tracks:
            print("No tracks matched your criteria.")
            return

    # Set final filepaths
    for item in selected_tracks:
        item["final_filepath"] = os.path.join(
            target_dir,
            item["album_folder"],
            item["genre"],
            f"{item['safe_title']}.mp3"
        )

    total_tracks = len(selected_tracks)
    print(f"\nProcessing {total_tracks} tracks into: {target_dir} (Threads: {args.threads})")

    # PHASE 1: BATCH DOWNLOAD
    print("\n--- PHASE 1: Batch Downloading All Tracks & Cover Images ---")
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for idx, item in enumerate(selected_tracks, start=1):
            executor.submit(download_track_resources, item, idx, total_tracks)

    # PHASE 2: BATCH CROP & RESIZE COVERS
    print("\n--- PHASE 2: Batch Cropping & Resizing Cover Images ---")
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for idx, item in enumerate(selected_tracks, start=1):
            executor.submit(process_track_cover, item, cover_size, idx, total_tracks)

    # PHASE 3: BATCH TAG & EXPORT
    print("\n--- PHASE 3: Batch Tagging & Outputting MP3s ---")
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for idx, item in enumerate(selected_tracks, start=1):
            executor.submit(tag_and_output_single_track, item, idx, total_tracks)

    print("\nBatch Processing Complete!")

if __name__ == "__main__":
    main()
