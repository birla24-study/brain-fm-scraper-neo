import os
import re
import json
import subprocess
import shutil
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

def get_json_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.json')]

def prompt_choice(options, prompt_text):
    print(f"\n{prompt_text}")
    for idx, opt in enumerate(options, 1):
        print(f"{idx}. {opt}")
    
    while True:
        try:
            choice = int(input("\nEnter your choice (number): "))
            if 1 <= choice <= len(options):
                return options[choice - 1]
            print("Invalid choice. Please select a valid number.")
        except ValueError:
            print("Please enter a valid number.")

def process_track(track, tmp_filepath, final_dir, cover_path):
    # Prepare metadata
    song_name = track.get("song_name", "Unknown")
    genre = track.get("genre", "Unknown")
    activity = track.get("activity", "Unknown")
    sub_activity = track.get("sub_activity", "Unknown")
    neural_effect = track.get("neural_effect", "Unknown")
    instrumentation = track.get("instrumentation", "")
    
    title = f"{song_name} ({genre})"
    artist = "BrainFM"
    album = f"{activity}:{sub_activity} ({neural_effect})"
    
    # Final output file path
    safe_title = "".join([c for c in title if c.isalpha() or c.isdigit() or c in ' -_()']).rstrip()
    final_filepath = os.path.join(final_dir, f"{safe_title}.mp3")
    
    # ffmpeg command to embed metadata and cover art
    # Using codec copy for fast processing without re-encoding
    cmd = [
        "ffmpeg", "-y", "-i", tmp_filepath
    ]
    
    has_cover = os.path.exists(cover_path)
    if has_cover:
        cmd.extend(["-i", cover_path, "-map", "0:0", "-map", "1:0"])
    else:
        cmd.extend(["-map", "0:0"])
        
    cmd.extend([
        "-c", "copy",
        "-id3v2_version", "3",
        "-metadata", f"title={title}",
        "-metadata", f"artist={artist}",
        "-metadata", f"album={album}",
        "-metadata", f"genre={genre}"
    ])
    
    if instrumentation:
        cmd.extend(["-metadata", f"comment={instrumentation}"])
    
    if has_cover:
        cmd.extend([
            "-metadata:s:v", "title=Album cover",
            "-metadata:s:v", "comment=Cover (front)"
        ])
        
    cmd.append(final_filepath)
    
    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        print(f"Processed: {title}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {title}: {e}")
    finally:
        # Clean up tmp file
        if os.path.exists(tmp_filepath):
            os.remove(tmp_filepath)

def main():
    base_dir = os.path.abspath(os.path.dirname(__file__))
    output_dir = os.path.join(base_dir, "output")
    covers_dir = os.path.join(base_dir, "covers")
    downloads_dir = os.path.join(base_dir, "downloads")
    tmp_dir = os.path.join(base_dir, "tmp_downloads")
    
    os.makedirs(downloads_dir, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)
    
    # Step 1: Select JSON file
    json_files = get_json_files(output_dir)
    if not json_files:
        print("No JSON files found in the output folder.")
        return
        
    selected_json = prompt_choice(json_files, "Select a JSON file to process:")
    json_path = os.path.join(output_dir, selected_json)
    
    with open(json_path, 'r', encoding='utf-8') as f:
        tracks = json.load(f)
        
    if not tracks:
        print("The selected JSON file is empty.")
        return
        
    for t in tracks:
        if "neural_effect" in t and isinstance(t["neural_effect"], str):
            t["neural_effect"] = re.sub(r'(?i)\s*neural effect', '', t["neural_effect"]).strip()
        
    # Step 2: Extract genres and prompt for genre
    genres = sorted(list(set(track.get("genre", "Unknown") for track in tracks)))
    options = ["All"] + genres
    selected_genre = prompt_choice(options, "Select a genre to download:")
    
    # Filter tracks by genre
    if selected_genre != "All":
        tracks = [t for t in tracks if t.get("genre", "Unknown") == selected_genre]
        
    # Extract neural effects and prompt for neural effect
    neural_effects = sorted(list(set(track.get("neural_effect", "Unknown") for track in tracks)))
    options_effects = ["All"] + neural_effects
    selected_effect = prompt_choice(options_effects, "Select a neural effect to download:")
    
    # Filter tracks by neural effect
    if selected_effect != "All":
        tracks_to_download = [t for t in tracks if t.get("neural_effect", "Unknown") == selected_effect]
    else:
        tracks_to_download = tracks
        
    print(f"\nFound {len(tracks_to_download)} tracks to download.")
    
    # Step 3: Prepare aria2c download list
    aria2_input_file = os.path.join(tmp_dir, "aria2_input.txt")
    
    pending_tracks = []
    with open(aria2_input_file, 'w', encoding='utf-8') as f:
        for idx, track in enumerate(tracks_to_download):
            url = track.get("url")
            if not url:
                continue
                
            # Check if file already exists
            song_name = track.get("song_name", "Unknown")
            genre = track.get("genre", "Unknown")
            activity = track.get("activity", "Unknown")
            sub_activity = track.get("sub_activity", "Unknown")
            neural_effect = track.get("neural_effect", "Unknown")
            
            title = f"{song_name} ({genre})"
            safe_title = "".join([c for c in title if c.isalpha() or c.isdigit() or c in ' -_()']).rstrip()
            final_dir = os.path.join(downloads_dir, f"{activity}:{sub_activity} ({neural_effect})", genre)
            final_filepath = os.path.join(final_dir, f"{safe_title}.mp3")
            
            if os.path.exists(final_filepath):
                print(f"Skipping already downloaded: {title}")
                continue
                
            pending_tracks.append((idx, track))
            
            # Remove query parameters to get raw extension
            url_no_query = url.split('?')[0]
            ext = url_no_query.split('.')[-1] if '.' in url_no_query.split('/')[-1] else 'mp3'
            f.write(f"{url}\n")
            f.write(f"  out=track_{idx}.{ext}\n")
            
    if not pending_tracks:
        print("\nAll selected tracks have already been downloaded.")
        if os.path.exists(aria2_input_file):
            os.remove(aria2_input_file)
        return
                
    # Step 4: Run aria2c
    print(f"\nStarting {len(pending_tracks)} downloads with aria2c...")
    aria2_cmd = [
        "aria2c",
        "-x", "16",  # max connections per server
        "-s", "16",  # max splits per file
        "-j", "16",  # max concurrent downloads
        "-U", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--dir", tmp_dir,
        "--input-file", aria2_input_file
    ]
    
    try:
        subprocess.run(aria2_cmd, check=True)
    except FileNotFoundError:
        print("Error: aria2c is not installed or not in PATH.")
        return
    except subprocess.CalledProcessError:
        print("Some downloads may have failed. Proceeding with downloaded files...")
        
    # Step 5: Process and Tag downloaded files
    print("\nProcessing and tagging files with ffmpeg...")
    
    # Using ThreadPoolExecutor for faster processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        for idx, track in pending_tracks:
            url = track.get("url")
            url_no_query = url.split('?')[0]
            ext = url_no_query.split('.')[-1] if '.' in url_no_query.split('/')[-1] else 'mp3'
            tmp_filepath = os.path.join(tmp_dir, f"track_{idx}.{ext}")
            
            if not os.path.exists(tmp_filepath):
                print(f"Missing downloaded file for: {track.get('song_name')} (track_{idx}.{ext})")
                continue
                
            activity = track.get("activity", "Unknown")
            sub_activity = track.get("sub_activity", "Unknown")
            genre = track.get("genre", "Unknown")
            neural_effect = track.get("neural_effect", "Unknown")
            
            # Covers logic (e.g., Focus.png)
            cover_filename = f"{activity}.png"
            cover_path = os.path.join(covers_dir, cover_filename)
            
            album_dir_name = f"{activity}:{sub_activity} ({neural_effect})"
            final_dir = os.path.join(downloads_dir, album_dir_name, genre)
            os.makedirs(final_dir, exist_ok=True)
            
            executor.submit(process_track, track, tmp_filepath, final_dir, cover_path)
            
    print("\nCleaning up...")
    if os.path.exists(aria2_input_file):
        os.remove(aria2_input_file)
        
    print("\nAll done! Check your 'downloads' folder.")

if __name__ == "__main__":
    main()
