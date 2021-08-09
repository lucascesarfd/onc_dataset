# onc_dataset
This repository contains the pipeline to obtain and process ONC data from hydrophones.

## Table of Contents

1. [Step 7](#step-7)

## Step 7

1. Read csv file to extract periods of timestamp;
2. Search on raw WAV files folder for the correct period of time;
3. Read the wav files and split into 1 minute normalized pieces of audio;
4. Group the pieces of audio with the period of ais files range;
5. Save into correct folder.

