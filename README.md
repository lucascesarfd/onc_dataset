# ONC Dataset Generation Pipeline
This repository contains the pipeline to obtain and process ONC data from hydrophones.
The dataset generation pipeline is divided in steps. Each step can be performed separately, but some of the steps are pre-requisites of the previous ones. 

**Attention:** Is **HIGHLY** recommended to have a large storage available (at least 2TB) to download the ONC WAV Files. 

## Environment Setup

A Dockerfile is available at this repository to simplify the environment setup. As the needed sources are only Python dependencies, a virtual environment can also be created.

To install the dependencies, run the following commands:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```
## Pipeline description
A brief pipeline description can be found below, separating the development into 13 steps:

### Step 0 - Query ONC deployments
1. Query the ONC server for the deployments of the choosen hydrophones;
2. Read the following information: recording begin, recording end, latitude, longitude, depth, and location;
3. Save the information into a `.csv` file.

### Step 1 - Download AIS files
1. Search for AIS data from the date choosen;
2. Download the `txt` files from ONC.

### Step 2 - Download audio files
1. Search for WAV data from the date choosen;
2. Download the `.wav` files from ONC.
**WARNING:** This step requires a lot of available disk memory. The smallest deployment have more than 1TB of audio data.

### Step 3 - Parse AIS to JSON
This function parse the ais messages downloaded from ONC into JSON files, filtering by the type of the messages and discarting messages without the needed values.
1. Find the downloaded `.txt` AIS files;
2. Keep only the relevant messages. They are: Position report, Static and voyage related data, Standard Class B equipment position report, Extended Class B equipment position report, and Static data report.
3. Filter from those messages, only a few informations: Positioning (x and y), SOG, COG, true heading, and type and cargo codes;
4. Save the corresponding information into a `.json` file.

### Step 4 - Clean AIS data
1. Read the `.json` files into dataframes;
2. Propagate the 'type_and_cargo' messages throughout the MMSI's;
3. Drop messages without positional coordinates and/or duplicates;
4. Calculate the distance from the hydrophone to the vessel;
5. Filter only the data that fits the choosen scenario;
6. Save the corresponding information into a `.feather` file.

### Step 5 - Combine deployment AIS data
1. Read the cleaned AIS files;
2. Removes the Vessel entries that have just one message;
3. Dump AIS data to a monolithic `.feather` file;
4. Generate a new data with linearly interpolated values to obtain more granularity;
5. Combine the raw and interpolated data frames;
6. Dump AIS interpolated data to a monolithic `.feather` file;

### Step 6 - Identify scenarios
1. Find all of the cleaned AIS files for each deployment;
2. Find the time intervals where only one vessel is within range;

### Step 7 - Classify WAV files from range
1. Read `.csv` file to extract periods of timestamp;
2. Search on raw WAV files folder for the correct period of time;
3. Read the wav files and split into 1 minute normalized pieces of audio;
4. Group the pieces of audio with the period of ais files range;
5. Save into correct folder.

### Step 8 - Download CTD files
1. Search for CTD data from the date choosen;
2. Download from ONC.

### Step 9 - Clean CTD files
1. Select only information of salinity, conductivity, temperature, pressure, and sound speed;
2. Save the corresponding information into a `.feather` file.

### Step 10 - Generate the metadata for the full dataset
1. Get the following information from each time period: *label*, *duration*, *file path*, *sample rate*, *class code*, *date*, *MMSI*;
2. Get also a average for the time period of the CTD data: *salinity*, *conductivity*, *temperature*, *pressure*, and *sound speed*;
3. Normalize the CTD information;
4. Save all the data into a `.csv` file.

### Step 11 - Generate a balanced version of the full dataset
1. Count the occurrences of each class;
2. Do a undersample strategy to crop the longer classes according to the smaller one;
3. Save all the data into a `.csv` file.

### Step 12 - Generate metadata for small periods of duration
1. Read the original metadata generated on [Step 10](#step-10);
2. Create a new column named *sub_init* to accomodate the time frame where this new entry will start;
3. Split the `.csv` row according with the duration choosen;
4. Create a new row for each new entry;
5. Save all the data into a `.csv` file.

### Step 13 - Split dataset into Train, Test and Validation
1. Read all the metadata;
2. Apply a random sort on the data;
3. Save all the data into three `.csv` files: *Train*, *Validation*, and *Test*.

## Reference
The results from this work were published at IEEE Access, at the following reference:

[An Investigation of Preprocessing Filters and Deep Learning Methods for Vessel Type Classification With Underwater Acoustic Data](https://ieeexplore.ieee.org/document/9940921)

```bibtex
@article{domingos2022investigation,
  author={Domingos, Lucas C. F. and Santos, Paulo E. and Skelton, Phillip S. M. and Brinkworth, Russell S. A. and Sammut, Karl},
  journal={IEEE Access}, 
  title={An Investigation of Preprocessing Filters and Deep Learning Methods for Vessel Type Classification With Underwater Acoustic Data}, 
  year={2022},
  volume={10},
  number={},
  pages={117582-117596},
  doi={10.1109/ACCESS.2022.3220265}}
```

A complete literature review containing the background knowledge of this work is available on the following reference:

[A Survey of Underwater Acoustic Data Classification Methods Using Deep Learning for Shoreline Surveillance](https://www.mdpi.com/1424-8220/22/6/2181)

```bibtex
@article{domingos2022survey,
  author={Domingos, Lucas C. F. and Santos, Paulo E. and Skelton, Phillip S. M. and Brinkworth, Russell S. A. and Sammut, Karl},
  title={A Survey of Underwater Acoustic Data Classification Methods Using Deep Learning for Shoreline Surveillance},
  volume={22},
  ISSN={1424-8220},
  url={http://dx.doi.org/10.3390/s22062181},
  DOI={10.3390/s22062181},
  number={6},
  publisher={MDPI AG},
  journal={Sensors},
  year={2022},
  month={Mar},
  pages={2181}
}
```
