import os
import json
import subprocess
import shutil
import re
import argparse
from concurrent.futures import ThreadPoolExecutor
import urllib.request
import urllib.parse
import threading
from PIL import Image

def get_json_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.json')]

def get_safe_filename(name):
    return "".join([c for c in name if c.isalpha() or c.isdigit() or c in ' -_()']).rstrip()

def slugify(name):
    return re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')

def find_cover_art(art_dir, song_name):
    if not os.path.exists(art_dir):
        return None
    slug = slugify(song_name)
    # Direct match check (.jpg, .jpeg, .png)
    for ext in ['.jpg', '.jpeg', '.png']:
        path = os.path.join(art_dir, f"{slug}{ext}")
        if os.path.exists(path):
            return path
    # Partial suffix check (e.g. slug-2.jpg)
    try:
        for filename in os.listdir(art_dir):
            if filename.startswith(slug) and filename.lower().endswith(('.jpg', '.jpeg', '.png')):
                return os.path.join(art_dir, filename)
    except Exception:
        pass
    return None

cover_lock = threading.Lock()

def download_cover_art(song_name, cover_url, art_dir):
    if not cover_url:
        return None
    
    slug = slugify(song_name)
    dest_path = os.path.join(art_dir, f"{slug}.jpg")
    
    if os.path.exists(dest_path):
        return dest_path
        
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
            
        new_query = urllib.parse.urlencode(new_params)
        modified_url = urllib.parse.urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment
        ))
        
        print(f"Downloading cover art for: {song_name}...", flush=True)
        os.makedirs(art_dir, exist_ok=True)
        
        req = urllib.request.Request(
            modified_url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            with open(dest_path, 'wb') as f:
                f.write(response.read())
                
        return dest_path
    except Exception as e:
        print(f"Error downloading cover art for {song_name}: {e}", flush=True)
        if os.path.exists(dest_path):
            try:
                os.remove(dest_path)
            except:
                pass
        return None

def get_cropped_and_resized_cover(cover_path, size):
    """
    Crops the center square of the image and resizes it to size x size.
    Saves the processed cover in a subfolder and returns the new path.
    """
    if not cover_path or not os.path.exists(cover_path):
        return None
    try:
        art_dir = os.path.dirname(cover_path)
        processed_dir = os.path.join(art_dir, "processed")
        os.makedirs(processed_dir, exist_ok=True)
        
        base_name = os.path.basename(cover_path)
        name, ext = os.path.splitext(base_name)
        dest_filename = f"{name}_{size}{ext}"
        dest_path = os.path.join(processed_dir, dest_filename)
        
        if os.path.exists(dest_path):
            return dest_path
            
        with Image.open(cover_path) as img:
            # Convert RGBA/LA or transparency palette to RGB if saving to JPG
            if ext.lower() in ['.jpg', '.jpeg']:
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
            resized = cropped.resize((size, size), Image.Resampling.LANCZOS)
            resized.save(dest_path)
            return dest_path
    except Exception as e:
        print(f"Error processing cover image {cover_path}: {e}")
        return cover_path

def extract_neural_effect(effect_str):
    if not effect_str: return "Unknown"
    return re.sub(r'(?i)\s*neural effect', '', effect_str).strip()

def download_track(track, old_filepath):
    source_url = track.get("url")
    if not source_url:
        return False

    os.makedirs(os.path.dirname(old_filepath), exist_ok=True)

    if os.path.exists(old_filepath):
        return True

    try:
        subprocess.run(
            [
                "aria2c",
                "-x16",
                "-s16",
                "--allow-overwrite=true",
                "--auto-file-renaming=false",
                "--dir", os.path.dirname(old_filepath),
                "--out", os.path.basename(old_filepath),
                source_url,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True
    except Exception as e:
        print(f"Error downloading {track.get('song_name', 'Unknown')}: {e}")
        if os.path.exists(old_filepath):
            os.remove(old_filepath)
        return False

def process_track(track, old_filepath, new_filepath, cover_path, new_album, title):
    tmp_output = new_filepath + ".tmp.mp3"
    
    cmd = ["ffmpeg", "-y", "-i", old_filepath]
    
    has_cover = cover_path and os.path.exists(cover_path)
    if has_cover:
        cmd.extend(["-i", cover_path, "-map", "0:a:0", "-map", "1:0"])
    else:
        cmd.extend(["-map", "0:a:0"])
        
    cmd.extend([
        "-c", "copy",
        "-id3v2_version", "3",
        "-metadata", f"title={title}",
        "-metadata", f"album={new_album}",
        "-metadata", "artist=BrainFM",
        "-metadata", "album_artist=BrainFM",
        "-metadata", f"genre={track.get('sub_activity', 'Unknown')}"
    ])
    
    comment_parts = []
    if track.get("genre"):
        comment_parts.append(f"Genre: {track.get('genre')}")
    if track.get("moods"):
        comment_parts.append(f"Moods: {track.get('moods')}")
    if track.get("instrumentation"):
        comment_parts.append(f"Instrumentation: {track.get('instrumentation')}")
    if track.get("complexity"):
        comment_parts.append(f"Complexity: {track.get('complexity')}")
    if track.get("brightness"):
        comment_parts.append(f"Brightness: {track.get('brightness')}")
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
        os.makedirs(os.path.dirname(new_filepath), exist_ok=True)
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        shutil.move(tmp_output, new_filepath)
        print(f"Processed: {title}", flush=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error processing {title}: {e}", flush=True)
        if os.path.exists(tmp_output):
            os.remove(tmp_output)
        return False

def download_and_process_track(track, old_filepath, new_filepath, art_dir, cover_size, new_album, title, track_number, total_tracks):
    song_name = track.get("song_name", "Unknown")
    print(f"[{track_number}/{total_tracks}] Downloading: {song_name}", flush=True)
    if not download_track(track, old_filepath):
        print(f"[{track_number}/{total_tracks}] Download failed: {song_name}", flush=True)
        return
    print(f"[{track_number}/{total_tracks}] Processing: {song_name}", flush=True)
    
    cover_path = None
    with cover_lock:
        cover_path = find_cover_art(art_dir, song_name)
        if not cover_path:
            cover_url = track.get("cover_url")
            cover_path = download_cover_art(song_name, cover_url, art_dir)
        if cover_path:
            cover_path = get_cropped_and_resized_cover(cover_path, cover_size)
            
    if not process_track(track, old_filepath, new_filepath, cover_path, new_album, title):
        print(f"[{track_number}/{total_tracks}] Failed: {song_name}", flush=True)

def main():
    parser = argparse.ArgumentParser(description="Process BrainFM tracks selectively.")
    parser.add_argument("-o", "--output", help="Destination folder for processed files.")
    parser.add_argument("-a", "--activity", help="Filter by activity (e.g., Focus). Case-insensitive.")
    parser.add_argument("-g", "--genre", help="Filter by genre (e.g., LoFi). Case-insensitive.")
    parser.add_argument("-s", "--size", type=int, default=1080, help="Size of the cover art (width and height in pixels). Default is 1080.")
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.dirname(__file__))
    raw_dir = os.path.join(base_dir, "raw")
    art_dir = os.path.join(base_dir, "art")
    
    if not os.path.exists(art_dir):
        print(f"Error: The directory '{art_dir}' does not exist.")
        return

    if not os.path.exists(raw_dir):
        print(f"Error: The directory '{raw_dir}' does not exist.")
        return

    json_files = get_json_files(raw_dir)
    tracks = []
    
    for jf in json_files:
        with open(os.path.join(raw_dir, jf), 'r', encoding='utf-8') as f:
            tracks.extend(json.load(f))

    if not tracks:
        print("No tracks found in the JSON metadata.")
        return

    # Check which tracks exist locally
    all_tracks_info = []
    for track in tracks:
        activity = track.get("activity", "Unknown")
        genre = track.get("genre", "Unknown")
        sub_activity = track.get("sub_activity", "Unknown")
        neural_effect_full = track.get("neural_effect", "Unknown")
        neural_effect = extract_neural_effect(neural_effect_full)
        song_name = track.get("song_name", "Unknown")
        
        old_album_folder = f"{activity}:{sub_activity} ({neural_effect})"
        title = f"{song_name} ({neural_effect})"
        safe_title = get_safe_filename(title)
        
        old_filepath = os.path.join(raw_dir, old_album_folder, genre, f"{safe_title}.mp3")
        new_album = f"{activity}: {sub_activity}"
        
        # Check local existence of raw file
        exists_local = os.path.exists(old_filepath)
        
        all_tracks_info.append({
            "track": track,
            "activity": activity,
            "sub_activity": sub_activity,
            "neural_effect": neural_effect,
            "song_name": song_name,
            "genre": genre,
            "old_filepath": old_filepath,
            "new_album": new_album,
            "title": title,
            "exists_local": exists_local
        })

    # Determine interactive mode
    is_interactive = args.output is None

    if is_interactive:
        print("=== Brain.fm Downloader Interactive Mode ===")
        # Prompt for output directory
        target_dir_input = input("Enter destination folder for processed files [default: processed_output]: ").strip()
        if not target_dir_input:
            target_dir_input = "processed_output"
        target_dir = os.path.abspath(target_dir_input)
        
        # Prompt for cover size
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
                    custom_size_input = input("Enter custom size (e.g., 500) [default: 1080]: ").strip()
                    if not custom_size_input:
                        cover_size = 1080
                        break
                    cover_size = int(custom_size_input)
                    if cover_size > 0:
                        break
                    print("Size must be a positive integer.")
                except ValueError:
                    print("Invalid input. Please enter a valid integer.")
        
        # Show stats on local files
        local_count = sum(1 for t in all_tracks_info if t["exists_local"])
        total_count = len(all_tracks_info)
        print(f"\nAuto-detect: Found {local_count} / {total_count} raw files locally downloaded in '{raw_dir}'.\n")
        
        print("Choose processing mode:")
        print("1. Process all tracks (download missing, process all)")
        print("2. Process only locally downloaded raw files (skip downloading)")
        print("3. Filter and select specific tracks (by Activity, Sub-activity, Genre)")
        
        mode = ""
        while mode not in ["1", "2", "3"]:
            mode = input("Select option (1-3): ").strip()
            
        selected_tracks = []
        only_local = False
        
        if mode == "1":
            selected_tracks = all_tracks_info
        elif mode == "2":
            selected_tracks = [t for t in all_tracks_info if t["exists_local"]]
            only_local = True
        elif mode == "3":
            # Ask if they want to filter locally downloaded only
            only_local_input = input("Only process locally downloaded files? (y/n) [default: n]: ").strip().lower()
            only_local = only_local_input == 'y'
            candidate_tracks = [t for t in all_tracks_info if t["exists_local"]] if only_local else all_tracks_info
            
            if not candidate_tracks:
                print("No candidate tracks available for the selection.")
                return

            # Selection prompts
            # 1. Select Activity
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
            
            selected_activity = None if act_choice == 0 else activities[act_choice - 1]
            if selected_activity:
                candidate_tracks = [t for t in candidate_tracks if t["activity"] == selected_activity]
                
            # 2. Select Sub-activity
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
            
            selected_sub = None if sub_choice == 0 else sub_activities[sub_choice - 1]
            if selected_sub:
                candidate_tracks = [t for t in candidate_tracks if t["sub_activity"] == selected_sub]
                
            # 3. Select Genre
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
            
            selected_genre = None if gen_choice == 0 else genres[gen_choice - 1]
            if selected_genre:
                candidate_tracks = [t for t in candidate_tracks if t["genre"] == selected_genre]
                
            selected_tracks = candidate_tracks

        if not selected_tracks:
            print("No tracks matched your criteria.")
            return

        # Summary and confirmation
        local_to_process = sum(1 for t in selected_tracks if t["exists_local"])
        remote_to_process = len(selected_tracks) - local_to_process
        
        print(f"\nSummary of tracks to process:")
        print(f"- Total: {len(selected_tracks)}")
        print(f"- Already downloaded (local): {local_to_process}")
        print(f"- To be downloaded: {remote_to_process}")
        
        confirm = input("\nProceed? (y/n) [default: y]: ").strip().lower()
        if confirm == 'n':
            print("Aborted.")
            return

    else:
        # Non-interactive mode
        target_dir = os.path.abspath(args.output)
        cover_size = args.size
        selected_tracks = []
        for t in all_tracks_info:
            # Apply user filters from CLI arguments
            if args.activity and args.activity.lower() != t["activity"].lower():
                continue
            if args.genre and args.genre.lower() != t["genre"].lower():
                continue
            selected_tracks.append(t)
        
        if not selected_tracks:
            print("No tracks matched your criteria.")
            return
        
        only_local = False

    # Process all selected tracks
    print(f"\nProcessing {len(selected_tracks)} tracks to: {target_dir}")
    
    tracks_to_submit = []
    for item in selected_tracks:
        if only_local and not item["exists_local"]:
            continue
            
        safe_album = get_safe_filename(item["new_album"].replace(":", " -"))
        safe_filename = f"{get_safe_filename(item['title'])}.mp3"
        new_filepath = os.path.join(target_dir, safe_album, safe_filename)
        
        tracks_to_submit.append({
            "track": item["track"],
            "old_filepath": item["old_filepath"],
            "new_filepath": new_filepath,
            "new_album": item["new_album"],
            "title": item["title"]
        })

    if not tracks_to_submit:
        print("No tracks to process.")
        return

    with ThreadPoolExecutor(max_workers=4) as executor:
        total_tracks = len(tracks_to_submit)
        for index, item in enumerate(tracks_to_submit, start=1):
            executor.submit(
                download_and_process_track, 
                item["track"], 
                item["old_filepath"], 
                item["new_filepath"], 
                art_dir,
                cover_size,
                item["new_album"],
                item["title"],
                index,
                total_tracks,
            )
                
    print("Done!")

if __name__ == "__main__":
    main()
