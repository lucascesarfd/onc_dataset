import os
import re
import time
from tqdm import tqdm
import geopy.distance
import multiprocessing 

import numpy as np
import pandas as pd

from functools import partial

from utils import (
    bcolors,
    ais_params,
    get_num_of_threads,
    get_hydrophone_deployments,
    read_messages_from_json_file,
    dump_data_frame_to_feather_file,
)


def _vectorised_distance_to_hydrophone(
    _hydrophone_x,
    _hydrophone_y,
    _vessel_x,
    _vessel_y,
    _inclusion_radius,
    _left_bound,
    _right_bound,
    _top_bound,
    _bottom_bound,
):

    # The data should be 'clean' by this point, but sanity check the input anyway.
    if np.isnan(_vessel_x) or np.isnan(_vessel_y):
        return np.nan

    # We do a quick dead reckoning distance calculation as that is faster than a full geodesic calculation.
    if (_bottom_bound <= _vessel_y <= _top_bound) and (
        _left_bound <= _vessel_x <= _right_bound
    ):
        # Then do a more precise geodesic distance calculation.
        distance = geopy.distance.geodesic(
            (_hydrophone_y, _hydrophone_x), (_vessel_y, _vessel_x)
        ).meters
        if (np.ceil(distance).astype("int")) <= int(_inclusion_radius):
            return distance
        else:
            return np.nan
    else:
        return np.nan


def distance_calculation_for_chunks(
    _deployment_longitude,
    _deployment_latitude,
    _inclusion_radius,
    _coarse_longitude_left_bound,
    _coarse_longitude_right_bound,
    _coarse_latitude_top_bound,
    _coarse_latitude_bottom_bound,
    _chunk,
):

    _chunk["distance_to_hydrophone"] = np.vectorize(_vectorised_distance_to_hydrophone)(
        _deployment_longitude,
        _deployment_latitude,
        _chunk[ais_params.X].values,
        _chunk[ais_params.Y].values,
        _inclusion_radius,
        _coarse_longitude_left_bound,
        _coarse_longitude_right_bound,
        _coarse_latitude_top_bound,
        _coarse_latitude_bottom_bound,
    )

    return _chunk


def clean_for_chunk(
    _inclusion_radius,
    _parsed_ais_files_directory,
    _clean_ais_data_directory,
    _deployment_latitude,
    _deployment_longitude,
    _coarse_longitude_left_bound,
    _coarse_longitude_right_bound,
    _coarse_latitude_top_bound,
    _coarse_latitude_bottom_bound,
    _file,
):

    # Read the JSON file into a Pandas DataFrame.
    json_file = os.path.join(
        _parsed_ais_files_directory, _file.replace(".txt", "_parsed.json")
    )
    data_frame = pd.DataFrame(read_messages_from_json_file(json_file))

    # Propagate the 'type_and_cargo' messages throughout the MMSI's.
    data_frame = data_frame.sort_values(by=["mmsi", "type_and_cargo"])
    data_frame["type_and_cargo"] = data_frame.groupby("mmsi")["type_and_cargo"].fillna(
        method="ffill"
    )

    # Drop messages where there are no positional coordinates.
    data_frame = data_frame[data_frame.x.notna() & data_frame.y.notna()]

    # Drop duplicate messages.
    data_frame.drop_duplicates(keep="first", inplace=True)

    # Calculate the distance from the hydrophone to the vessel.
    distance_calculation_for_chunks(
        _deployment_longitude,
        _deployment_latitude,
        _inclusion_radius,
        _coarse_longitude_left_bound,
        _coarse_longitude_right_bound,
        _coarse_latitude_top_bound,
        _coarse_latitude_bottom_bound,
        data_frame,
    )

    # Take all vessels that are within the inclusion_radius specified.
    data_frame = data_frame[data_frame.distance_to_hydrophone.notnull()]

    # Create a new column that is the Pandas Timestamp.
    # Some things require AIS, some things require Pandas; annoying.
    data_frame["pd_timestamp"] = pd.to_datetime(data_frame["ais_timestamp"])

    # Out it goes.
    feather_file = os.path.join(
        _clean_ais_data_directory, _file.replace("_parsed.json", "_cleaned.feather")
    )
    dump_data_frame_to_feather_file(feather_file, data_frame)


def clean_ais_data(
    deployment_directory,
    parsed_ais_directory,
    clean_ais_directory,
    _inclusion_radius,
    use_all_threads=False,
):
    '''
    This function produces the feather files from the JSON inputed files
    according some restrictions. The new feather file will contain only
    data there is within te inclusion radius and that have positional data.
    '''

    # Threading differences between systems.
    number_of_threads = get_num_of_threads(use_all_threads)

    # Read in the hydrophone deployments as we will treat each deployment as an individual dataset.
    hydrophone_deployments = get_hydrophone_deployments(deployment_directory)

    print(f"Finding available JSON files to clean...")
    # List available JSON files to clean in the input folder.
    available_files = os.listdir(parsed_ais_directory)
    available_files.sort()
    print(f"  Found {bcolors.BOLD}{len(available_files)}{bcolors.ENDC} JSON files to clean")

    # List existing cleaned files in the destination folder.
    existing_files = os.listdir(clean_ais_directory)
    existing_files.sort()
    print(f"  Found {bcolors.BOLD}{len(existing_files)}{bcolors.ENDC} existing Feather files")

    print(f"Working out what files need cleaning...")
    files_to_clean = [file for file in available_files if re.sub("_parsed.json", "_cleaned.feather", file) not in existing_files]
    print(f"  There are {bcolors.BOLD}{len(files_to_clean)}{bcolors.ENDC} files to clean")
    files_to_clean.sort(
        key=lambda f: os.stat(os.path.join(parsed_ais_directory, f)).st_size,
        reverse=False,
    )

    # If there is no files to clean, terminate the execution.
    if not files_to_clean:
        print(f"{bcolors.WARNING}No files to clean.{bcolors.ENDC}")
        return

    for device in hydrophone_deployments.keys():
        for deployment in hydrophone_deployments[device].itertuples(index=False):

            # Get begin and end information from deployment files.
            deployment_begin = pd.Timestamp(deployment.begin).normalize()
            deployment_end = pd.Timestamp(deployment.end).normalize() + pd.DateOffset(
                days=1
            )

            deployment_ais_data_files = []

            for file in files_to_clean:
                file_timestamp = pd.Timestamp(file.split("_")[1])

                if deployment_begin <= file_timestamp <= deployment_end:
                    deployment_ais_data_files.append(file)

            # We initially do a quick dead-reckoning of distance by using a square around the area of interest.
            # This will produce around 20% erroneous results (circle within a square).
            # This is done as a full geodesic distance calculation is far more computational expensive.
            coarse_offset = _inclusion_radius + 2000.0
            earth_curvature = 6378137.0

            latitude_offset = (coarse_offset / earth_curvature) * 180.0 / np.pi
            coarse_latitude_top_bound = deployment.latitude + latitude_offset
            coarse_latitude_bottom_bound = deployment.latitude - latitude_offset

            longitude_offset = (
                (
                    coarse_offset
                    / (earth_curvature * np.cos(np.pi * deployment.longitude / 180.0))
                )
                * 180.0
                / np.pi
            )
            coarse_longitude_left_bound = deployment.longitude + longitude_offset
            coarse_longitude_right_bound = deployment.longitude - longitude_offset

            # Clean the data.
            print("Cleaning AIS data from deployment...")
            threading_pool = multiprocessing.Pool(processes=number_of_threads)
            function_partial = partial(
                clean_for_chunk,
                _inclusion_radius,
                parsed_ais_directory,
                clean_ais_directory,
                deployment.latitude,
                deployment.longitude,
                coarse_longitude_left_bound,
                coarse_longitude_right_bound,
                coarse_latitude_top_bound,
                coarse_latitude_bottom_bound,
            )

            for _ in tqdm(threading_pool.imap_unordered(function_partial, deployment_ais_data_files, chunksize=1), total=len(deployment_ais_data_files)):
                pass

            threading_pool.close()
            threading_pool.join()
