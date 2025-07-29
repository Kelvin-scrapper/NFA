# orchestrator.py

import os
import shutil
from main import NFAMADownloader
from map import NfaProcessor

# --- Configuration ---
# Define the directories for the pipeline.
# The downloader will save files here, and the processor will read from here.
DOWNLOAD_DIRECTORY = "nfama_data" 
# The processor will save its final ZIP report here.
OUTPUT_DIRECTORY = "output"

def clear_directory(directory_path):
    """Removes all files and subdirectories in a given directory."""
    if not os.path.exists(directory_path):
        print(f"Directory '{directory_path}' does not exist. Skipping cleanup.")
        return
    
    print(f"Cleaning up directory: '{directory_path}'...")
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')
    print("Cleanup complete.")


def run_pipeline():
    """
    Executes the full data pipeline:
    1. Cleans up previous run's artifacts.
    2. Downloads the source Excel files using the NFAMADownloader.
    3. Processes the downloaded files using the NfaProcessor.
    """
    print("="*60)
    print("🚀 STARTING NFA DATA PIPELINE")
    print("="*60)

    # --- Cleanup Step (Optional but Recommended) ---
    # Clean the download and output directories from previous runs.
    clear_directory(DOWNLOAD_DIRECTORY)
    clear_directory(OUTPUT_DIRECTORY)

    # --- Step 1: Download Files ---
    print("\n--- [STEP 1/2] INITIATING DOWNLOAD PROCESS ---")
    downloader = NFAMADownloader(download_path=DOWNLOAD_DIRECTORY)
    download_success = downloader.run()

    if not download_success:
        print("\n❌ PIPELINE HALTED: File download failed. Please check the logs.")
        return # Stop the pipeline if downloads fail

    print("\n✅ Download step completed successfully.")

    # --- Step 2: Process Files ---
    print("\n--- [STEP 2/2] INITIATING DATA PROCESSING ---")
    try:
        processor = NfaProcessor()
        # The processor will scan the DOWNLOAD_DIRECTORY and save results to OUTPUT_DIRECTORY
        processor.process_directory(scan_dir=DOWNLOAD_DIRECTORY, output_dir=OUTPUT_DIRECTORY)
    except Exception as e:
        print(f"\n❌ PIPELINE HALTED: An error occurred during data processing: {e}")
        return

    print("\n" + "="*60)
    print("🎉 NFA DATA PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"📦 Final report is available in the '{OUTPUT_DIRECTORY}' directory.")
    print("="*60)

if __name__ == "__main__":
    run_pipeline()