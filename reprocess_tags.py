import os
import subprocess
import shutil
from concurrent.futures import ThreadPoolExecutor

def process_track(filepath, cover_path, album_name):
    tmp_output = filepath + ".tmp.mp3"
    
    cmd = [
        "ffmpeg", "-y", "-i", filepath
    ]
    
    has_cover = os.path.exists(cover_path)
    if has_cover:
        cmd.extend(["-i", cover_path, "-map", "0:a:0", "-map", "1:0"])
    else:
        cmd.extend(["-map", "0:a:0"])
        
    cmd.extend([
        "-c", "copy",
        "-id3v2_version", "3",
        "-metadata", f"album={album_name}"
    ])
    
    if has_cover:
        cmd.extend([
            "-metadata:s:v", "title=Album cover",
            "-metadata:s:v", "comment=Cover (front)",
            "-disposition:v", "attached_pic"
        ])
        
    cmd.append(tmp_output)
    
    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        # Overwrite the original file with the tagged version
        shutil.move(tmp_output, filepath)
        print(f"Processed and tagged: {os.path.basename(filepath)}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {os.path.basename(filepath)}: {e}")
        if os.path.exists(tmp_output):
            os.remove(tmp_output)

def main():
    base_dir = os.path.abspath(os.path.dirname(__file__))
    downloads_dir = os.path.join(base_dir, "downloads")
    covers_dir = os.path.join(base_dir, "covers")
    
    if not os.path.exists(downloads_dir):
        print(f"Error: The directory '{downloads_dir}' does not exist.")
        return

    if not os.path.exists(covers_dir):
        print(f"Error: The directory '{covers_dir}' does not exist.")
        return

    tracks_to_process = []

    for root, dirs, files in os.walk(downloads_dir):
        for file in files:
            if not file.lower().endswith('.mp3'):
                continue

            filepath = os.path.join(root, file)
            
            # Determine relative path to identify the album folder
            rel_path = os.path.relpath(root, downloads_dir)
            path_parts = rel_path.split(os.sep)

            # Ensure the file is inside the expected Album/Genre structure
            if len(path_parts) < 2:
                continue

            album_folder_name = path_parts[0]
            
            # Extract the activity (e.g., "Focus" from "Focus: Deep Work - High")
            if ":" not in album_folder_name:
                continue
                
            activity = album_folder_name.split(":")[0].strip()
            
            cover_filename = f"{activity.capitalize()}.png"
            cover_path = os.path.join(covers_dir, cover_filename)
                
            tracks_to_process.append({
                "filepath": filepath,
                "cover_path": cover_path,
                "album_name": album_folder_name
            })

    print(f"Found {len(tracks_to_process)} tracks to process.")
    print("Applying ID3 tags and cover art...")
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        for item in tracks_to_process:
            executor.submit(
                process_track, 
                item["filepath"], 
                item["cover_path"], 
                item["album_name"]
            )
                    
    print("Done!")

if __name__ == "__main__":
    main()
