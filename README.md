# seaTracks
Code to compile and process satellite tracking and diving datasets retrieved from the SMRU portal

<further description pending>
  
The code is laid out in several small scripts that each perform a single task. When starting from scratch, the scripts should be run in the following order:
  1. download-raw-datasets # This script interfaces with the SMRU portal to download available campaigns as laid out in the "available_campaigns.csv". Note, this requires login details.
  2. unzip_files # This script unzips each of the downloaded files and saves the ACCESS databases into the "access_files" sub-directory
  3. compile_datasets # This script extracts all available location and dive data from the ACCESS databases and compiles them into two master datasets
  4. process_tracks # This script processes the location data using a state-space model from the foieGras package 
  
Additional scripts  
  4. visual_qc_loc_data # This exports some simple diagnostic plots of each deployment for visual quality checking - plots are added to the "loc_qc_plots" sub-directory in "compiled_datasets"
  5. cross-ref_loc-dive-and-meta # Is under development for cross-referencing between the location/dive datasets and the metadata
