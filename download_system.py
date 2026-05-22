import os
import json
import subprocess
import shutil
import re
import argparse
from concurrent.futures import ThreadPoolExecutor

def get_json_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.json')]

def get_safe_filename(name):
    return "".join([c for c in name if c.isalpha() or c.isdigit() or c in ' -_()']).rstrip()

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

def process_track(track, old_filepath, new_filepath, cover_path, new_album):
    song_name = track.get("song_name", "Unknown")
    genre = track.get("genre", "Unknown")
    
    title = f"{song_name} ({genre})"
    tmp_output = new_filepath + ".tmp.mp3"
    
    cmd = ["ffmpeg", "-y", "-i", old_filepath]
    
    has_cover = os.path.exists(cover_path)
    if has_cover:
        cmd.extend(["-i", cover_path, "-map", "0:a:0", "-map", "1:0"])
    else:
        cmd.extend(["-map", "0:a:0"])
        
    cmd.extend([
        "-c", "copy",
        "-id3v2_version", "3",
        "-metadata", f"album={new_album}"
    ])
    
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

def download_and_process_track(track, old_filepath, new_filepath, cover_path, new_album, track_number, total_tracks):
    song_name = track.get("song_name", "Unknown")
    print(f"[{track_number}/{total_tracks}] Downloading: {song_name}", flush=True)
    if not download_track(track, old_filepath):
        print(f"[{track_number}/{total_tracks}] Download failed: {song_name}", flush=True)
        return
    print(f"[{track_number}/{total_tracks}] Processing: {song_name}", flush=True)
    if not process_track(track, old_filepath, new_filepath, cover_path, new_album):
        print(f"[{track_number}/{total_tracks}] Failed: {song_name}", flush=True)

def main():
    parser = argparse.ArgumentParser(description="Process BrainFM tracks selectively.")
    parser.add_argument("-o", "--output", required=True, help="Destination folder for processed files.")
    parser.add_argument("-a", "--activity", help="Filter by activity (e.g., Focus). Case-insensitive.")
    parser.add_argument("-g", "--genre", help="Filter by genre (e.g., LoFi). Case-insensitive.")
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.dirname(__file__))
    output_dir = os.path.join(base_dir, "output")
    downloads_dir = os.path.join(base_dir, "downloads")
    covers_dir = os.path.join(base_dir, "covers")
    target_dir = os.path.abspath(args.output)
    
    if not os.path.exists(covers_dir):
        print(f"Error: The directory '{covers_dir}' does not exist.")
        return

    if not os.path.exists(output_dir):
        print(f"Error: The JSON directory '{output_dir}' does not exist.")
        return

    json_files = get_json_files(output_dir)
    tracks = []
    
    for jf in json_files:
        with open(os.path.join(output_dir, jf), 'r', encoding='utf-8') as f:
            tracks.extend(json.load(f))
            
    tracks_to_process = []
    
    for track in tracks:
        activity = track.get("activity", "Unknown")
        genre = track.get("genre", "Unknown")

        # Apply user filters
        if args.activity and args.activity.lower() != activity.lower():
            continue
        if args.genre and args.genre.lower() != genre.lower():
            continue

        sub_activity = track.get("sub_activity", "Unknown")
        neural_effect_full = track.get("neural_effect", "Unknown")
        neural_effect = extract_neural_effect(neural_effect_full)
        song_name = track.get("song_name", "Unknown")
        
        old_album_folder = f"{activity}:{sub_activity} ({neural_effect})"
        title = f"{song_name} ({genre})"
        safe_title = get_safe_filename(title)
        
        old_filepath = os.path.join(downloads_dir, old_album_folder, genre, f"{safe_title}.mp3")
        
        new_album = f"{activity}: {sub_activity} - {neural_effect}"
        new_filepath = os.path.join(target_dir, new_album, genre, f"{safe_title}.mp3")
        
        cover_filename = f"{activity.capitalize()}.png"
        cover_path = os.path.join(covers_dir, cover_filename)
            
        tracks_to_process.append({
            "track": track,
            "old_filepath": old_filepath,
            "new_filepath": new_filepath,
            "cover_path": cover_path,
            "new_album": new_album
        })

    if not tracks_to_process:
        print("No tracks matched your criteria or no local files found.")
        return

    print(f"Found {len(tracks_to_process)} tracks matching criteria. Processing to: {target_dir}")
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        total_tracks = len(tracks_to_process)
        for index, item in enumerate(tracks_to_process, start=1):
            executor.submit(
                download_and_process_track, 
                item["track"], 
                item["old_filepath"], 
                item["new_filepath"], 
                item["cover_path"], 
                item["new_album"],
                index,
                total_tracks,
            )
                
    print("Done!")

if __name__ == "__main__":
    main()
