import os

import pandas as pd

from tqdm import tqdm
from pydub import AudioSegment
from datetime import timedelta
from utils import create_dir, zulu_string_to_datetime, pandas_timestamp_to_onc_format


def find_in_range_wav(data_from_range, wav_file_list):
    # Define project constants.
    five_minutes = timedelta(minutes = 5)
    to_download = []
    wav_file_list_datetime = []
    for wav_file in wav_file_list:
        wav_timestamp = os.path.splitext(wav_file)[0].split("_")[-1]
        wav_file_list_datetime.append(zulu_string_to_datetime(wav_timestamp))

    for begin, end in tqdm(zip(data_from_range.begin, data_from_range.end), total=len(data_from_range.index)):
        temp_datetime_wav = []
        ais_begin_datetime = zulu_string_to_datetime(begin)
        ais_end_datetime = zulu_string_to_datetime(end)

        for wav_file, wav_datetime in zip(wav_file_list, wav_file_list_datetime):
            if (wav_datetime >= (ais_begin_datetime - five_minutes)) and (wav_datetime <= ais_end_datetime):
                temp_datetime_wav.append((wav_datetime, wav_file))
                to_download.append(wav_file)

        if len(temp_datetime_wav) == 0:
            continue

        temp_datetime_wav.sort()
    to_download.sort()
    return to_download


def split_and_save_wav(raw_wav_directory, output_save_dir, data_from_range, wav_file_names, inclusion_radius=0, interval_ais_data_directory=''):
    # Define project constants.
    five_minutes = timedelta(minutes = 5)
    csv_data_to_fetch = []

    wav_file_list_datetime = []
    for wav_file in wav_file_names:
        wav_timestamp = os.path.splitext(wav_file)[0].split("_")[-1]
        wav_file_list_datetime.append(zulu_string_to_datetime(wav_timestamp))

    for file_idx, (begin, end) in enumerate(tqdm(zip(data_from_range.begin, data_from_range.end), total=len(data_from_range.index))):
        wav_files_in_range = []
        ais_begin_datetime = zulu_string_to_datetime(begin)
        ais_end_datetime = zulu_string_to_datetime(end)

        for wav_file, wav_datetime in zip(wav_file_names, wav_file_list_datetime):
            if (wav_datetime >= (ais_begin_datetime - five_minutes)) and (wav_datetime <= ais_end_datetime):
                wav_files_in_range.append((wav_datetime, wav_file))

        if len(wav_files_in_range) == 0:
            continue

        wav_files_in_range.sort()

        try:
            audio_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[0][1]))
            start_time = (ais_begin_datetime - wav_files_in_range[0][0]).total_seconds() * 1000
            audio_segment = audio_segment[start_time:]

            for idx, (wav_datetime, wav_file_name) in enumerate(wav_files_in_range[:-1]):
                if idx == 0:
                    continue
                audio_segment += AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_file_name))

            last_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[-1][1]))
            end_time = (ais_end_datetime - wav_files_in_range[-1][0]).total_seconds() * 1000
            audio_segment += last_segment[:end_time]

            audio_segment.export(os.path.join(output_save_dir, str(file_idx) + ".wav"), format="wav")
            csv_data_to_fetch.append(
                    (
                        pandas_timestamp_to_onc_format(ais_begin_datetime),
                        pandas_timestamp_to_onc_format(ais_end_datetime),
                        str(file_idx),
                    )
                )
        except:
            print(f"Error while exporting {file_idx} audio segment")

    interval_csv_file = open(os.path.join(output_save_dir, "intervals.csv"), "w")
    interval_csv_file.write("begin,end,wav_file\n")

    for interval in csv_data_to_fetch:
        interval_csv_file.write(",".join(str(entry) for entry in interval) + "\n")

    interval_csv_file.close()


def group_wav_from_range(classified_wav_directory, scenario_interval_dir, interval_ais_data_directory, raw_wav_directory, inclusion_radius):

    # Define exclusion range as an offset from the inclusion.
    exclusion_radius = 2000 + inclusion_radius

    directory_name = "inclusion_" + str(inclusion_radius) + "_exclusion_" + str(exclusion_radius)
    range_directory = create_dir(classified_wav_directory, directory_name)

    # Get a list of the WAV file names.
    wav_file_names = os.listdir(raw_wav_directory)

    interval_file_names = os.listdir(scenario_interval_dir)

    # Read vessel intervals range data from csv.
    vessel_interval_file_names = [file for file in interval_file_names if file.lower().endswith('unique_vessel_intervals.csv')]
    vessel_interval_data = pd.read_csv(os.path.join(scenario_interval_dir, vessel_interval_file_names[0]))

    # Get data from only the selected inclusion/exclusion range.
    vessel_data_from_range = vessel_interval_data[vessel_interval_data["exclusion_radius"] == exclusion_radius]

    # Identify, split, and save corresponding wav files into the directory.
    print(f"Generating and saving unique vessel within range files.")
    vessel_save_dir = create_dir(range_directory, "vessel")
    split_and_save_wav(raw_wav_directory, vessel_save_dir, vessel_data_from_range, wav_file_names, inclusion_radius, interval_ais_data_directory)
    
    # Read background range data from csv.
    background_interval_file_names = [file for file in interval_file_names if file.lower().endswith('background_intervals.csv')]
    background_interval_data = pd.read_csv(os.path.join(scenario_interval_dir, background_interval_file_names[0]))

    # Get data from only the selected inclusion/exclusion range.
    background_data_from_range = background_interval_data[background_interval_data["exclusion_radius"] == exclusion_radius]

    # Identify, split, and save corresponding wav files into the directory.
    print(f"Generating and saving background files.")
    background_save_dir = create_dir(range_directory, "background")
    split_and_save_wav(raw_wav_directory, background_save_dir, background_data_from_range, wav_file_names)
