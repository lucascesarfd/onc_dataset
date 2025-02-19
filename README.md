# ONC Dataset Generation Pipeline
This repository contains the pipeline to obtain and process ONC data from hydrophones.

In order to run this code, you will need a token from the ONC API. It is freely available once you have signed up in the [Oceans 3.0 API](https://data.oceannetworks.ca/Login). 

**Hint:** You can get your token from the Web Services API tab in your profile. Please refer to [Oceans 3.0 API Home](https://wiki.oceannetworks.ca/display/O2A/Oceans+3.0+API+Home) for the complete documentation

The dataset generation pipeline is divided in steps. Each step can be performed separately, but some of the steps are pre-requisites of the next ones. 

**Attention:** Is **HIGHLY** recommended to have a large storage available to download the ONC WAV Files. 

## Environment Setup

A Dockerfile is available at this repository to simplify the environment setup. As the needed sources are only Python dependencies, a virtual environment can also be created.

To install the dependencies, run the following commands:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## How to run
To generate a complete dataset you have to run the pipeline steps that are related to your needs. The `config.py` file contains all the setting needed to adapt the dataset generation pipeline. 

If you need data from other deployments than the ones used in this project, please refer to the [ONC Data Search](https://data.oceannetworks.ca/DataSearch) website and find the desired device codes. You can alter this codes in the `config.py` file.

Steps 0 to 7 are the basic steps to download and process the AIS data, the WAV files and syncronize them. Steps 8 and 9 are *optional* and only needed if the CTD information is desired. To generate a metadata with the annotations in a `.csv` format, you should run the step 10. 

**Examples:**

*To run a pipeline with the CTD information:*

In the config.py file, change the `STEPS` variable to `STEPS=[0,1,2,3,4,5,6,7,8,9,10]` and set `USE_CTD=True`. Then run:
```sh
python src/main.py
```

*To run a pipeline without the CTD information:*

In the config.py file, change the `STEPS` variable to `STEPS=[0,1,2,3,4,5,6,7,10]` (removing 8 and 9) and set `USE_CTD=False`. Then run:
```sh
python src/main.py
```

**PS:** After stage 0 you will have a `.csv` file containing the information about different deployments of the desired devices (hydrophones). Always check if the coordinates and dates are the desired ones. You can directly delete rows that are not needed for your specific application. The remaining of the pipeline will use **all**  the deployments specified in the `.csv` files.

Steps 11, 12, and 13 are only metadata management. They will balance the metadata, split into smaller segments, and split the data into train, validation and test subsets. Note that this won't affect your downloaded data (AIS, WAV, and CTD), it will only produce a new `.csv` metadata file with the new configuration.

## Pipeline description
A brief pipeline description can be found below, splitting the process into 13 steps:

### Step 0 - Query ONC deployments
1. Query the ONC server for the deployments of the choosen hydrophones;
2. Read the following information: recording begin, recording end, latitude, longitude, depth, and location;
3. Save the information into a `.csv` file.

### Step 1 - Download AIS files
1. Search for AIS data from the date choosen;
2. Download the `txt` files from ONC.

### Step 2 - Parse AIS to JSON
This function parse the ais messages downloaded from ONC into JSON files, filtering by the type of the messages and discarting messages without the needed values.
1. Find the downloaded `.txt` AIS files;
2. Keep only the relevant messages. They are: Position report, Static and voyage related data, Standard Class B equipment position report, Extended Class B equipment position report, and Static data report.
3. Filter from those messages only a few informations: Positioning (x and y), SOG, COG, true heading, and type and cargo codes;
4. Save the corresponding information into a `.json` file.

### Step 3 - Clean AIS data
1. Read the `.json` files into dataframes;
2. Propagate the 'type_and_cargo' messages throughout the MMSI's;
3. Drop messages without positional coordinates and/or duplicates;
4. Calculate the distance from the hydrophone to the vessel;
5. Filter only the data that fits the choosen scenario;
6. Save the corresponding information into a `.feather` file.

### Step 4 - Combine deployment AIS data
1. Read the cleaned AIS files;
2. Removes the Vessel entries that have just one message;
3. Dump AIS data to a monolithic `.feather` file;
4. Generate a new data with linearly interpolated values to obtain more granularity;
5. Combine the raw and interpolated data frames;
6. Dump AIS interpolated data to a monolithic `.feather` file;

### Step 5 - Identify scenarios
1. Find all of the cleaned AIS files for each deployment;
2. Find the time intervals where only one vessel is within range;

### Step 6 - Download audio files
1. Search for WAV data from the chosen scenario;
2. Download the `.wav` files from ONC.
   
**WARNING:** The need for disk memory is dependent of the size of you deployment and the date chosen. Be sure to have enough space

### Step 7 - Classify WAV files from range
1. Read `.csv` file to extract periods of timestamp;
2. Search on raw WAV files folder for the correct period of time;
3. Read the wav files and split into 1 minute normalized pieces of audio;
4. Group the pieces of audio with the period of ais files range;
5. Save into correct folder.

### (Optional) Step 8 - Download CTD files
1. Search for CTD data from the date choosen;
2. Download from ONC.

### (Optional) Step 9 - Clean CTD files
1. Select only information of salinity, conductivity, temperature, pressure, and sound speed;
2. Save the corresponding information into a `.feather` file.

### Step 10 - Generate the metadata for the full dataset
1. Get the following information from each time period: *label*, *duration*, *file path*, *sample rate*, *class code*, *date*, *MMSI*;
2. Get also a average for the time period of the CTD data: *salinity*, *conductivity*, *temperature*, *pressure*, and *sound speed*;
3. Normalize the CTD information;
4. Save all the data into a `.csv` file.

### (Optional) Step 11 - Generate a balanced version of the full dataset
1. Count the occurrences of each class;
2. Do a undersample strategy to crop the longer classes according to the smaller one;
3. Save all the data into a `.csv` file.

### (Optional) Step 12 - Generate metadata for small periods of duration
1. Read the original metadata generated on [Step 10](#step-10);
2. Create a new column named *sub_init* to accomodate the time frame where this new entry will start;
3. Split the `.csv` row according with the duration choosen;
4. Create a new row for each new entry;
5. Save all the data into a `.csv` file.

### (Optional) Step 13 - Split dataset into Train, Test and Validation
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

## Acknowledgements

This code, as well as the pipeline formulation and the code used as the basis, was developed in collaboration with [Phillip Skelton](phillip.skelton@flinders.edu.au). 

Thanks to [Paulo Santos](paulo.santos@flinders.edu.au) for the guidance and participation in this project.
