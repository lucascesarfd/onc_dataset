import os
import time
import os.path
import multiprocessing


import pandas as pd

from tqdm import tqdm
from onc.onc import ONC
from functools import partial

from utils import bcolors
from format import find_in_range_wav
from config import AIS_CODE, WAV_DEVICES, CTD_DEVICE


def get_deployment_filters(deployment_directory, filter_type="WAV"):
    filters = []

    for hydrophone_file in os.listdir(deployment_directory):
        hydrophone_name = hydrophone_file.split(".")[0]
        hydrophone_file_path = os.path.join(deployment_directory, hydrophone_file)
        hydrophone_deployment = pd.read_csv(hydrophone_file_path)
        for index, row in hydrophone_deployment.iterrows():
            if filter_type.lower() == "wav":
                filters.append(
                    {
                        "deviceCode": hydrophone_name,
                        "dateFrom": row["begin"],
                        "dateTo": row["end"],
                        "extension": "wav",
                    }
                )
            elif filter_type.lower() == "ais":
                filters.append(
                    {
                        "deviceCode": AIS_CODE,
                        "dateFrom": row["begin"],
                        "dateTo": row["end"],
                        "extension": "txt",
                    }
                )
            elif filter_type.lower() == "ctd":
                filters.append(
                    {
                        "deviceCode": CTD_DEVICE,
                        "dateFrom": row["begin"],
                        "dateTo": row["end"],
                        "extension": "txt",
                    }
                )
    return filters


def download_onc_file(_filename, _token, _path):
        onc_api = ONC(_token, outPath=_path, timeout=600)
        onc_api.getFile(_filename, overwrite=False)


def download_file_list(output_directory, token, files_to_download):
    start_time = time.time()
    thread_pool = multiprocessing.Pool(5)
    arguments = partial(download_onc_file, _token=token, _path=output_directory)
    for _ in tqdm(thread_pool.imap(arguments, files_to_download), total=len(files_to_download)):
        pass
    thread_pool.close()
    thread_pool.join()
    print(
        "  This download took {0:.3f} seconds to complete.\n".format(
            time.time() - start_time
        )
    )

    return


def query_onc_deployments(deployment_directory, token):
    # Instantiate ONC object.
    onc_api = ONC(token, outPath=deployment_directory, timeout=600)

    # Find the deployment windows for the hydrophones.
    filters = WAV_DEVICES
    results = []

    for new_filter in filters:
        results.extend(onc_api.getDeployments(new_filter))

    data_to_fetch = {}

    for result in sorted(results, key=lambda kv: kv["begin"]):
        if not result["begin"] or not result["end"]:
            continue

        else:
            if result["deviceCode"] not in data_to_fetch:
                data_to_fetch[result["deviceCode"]] = []

            data_to_fetch[result["deviceCode"]].append(
                (
                    result["begin"],
                    result["end"],
                    result["lat"],
                    result["lon"],
                    result["depth"],
                    result["locationCode"],
                )
            )

    filters = []

    for device_code, intervals in data_to_fetch.items():
        output_file = open(
            os.path.join(deployment_directory, f"{device_code}.csv"), "w"
        )
        output_file.write("begin,end,latitude,longitude,depth,location\n")

        for interval in intervals:
            output_file.write(",".join(str(entry) for entry in interval) + "\n")

        output_file.close()
    return


def download_files(output_directory, deployment_directory, token, file_type="WAV"):
    # Instantiate ONC object.
    onc_api = ONC(token, timeout=600)

    # Get the desired filter object to query for files at ONC servers.
    filters = get_deployment_filters(deployment_directory, filter_type=file_type)

    print(f"Finding available {file_type} files to download...")
    available_files = []
    for new_filter in filters:
        try:
            files_dict = onc_api.getListByDevice(new_filter, allPages=True)
            available_files.extend(files_dict["files"])
        except Exception as e:
            print(f"  {bcolors.WARNING}Error when running for {new_filter['deviceCode']} ({new_filter['dateFrom']} - {new_filter['dateTo']}){bcolors.ENDC}")
            with open(os.path.join(output_directory,"00_log_errors.txt"), "+a") as f:
                f.write(f"filter: {new_filter}\n\t{e}\n")


    available_files.sort()
    print(
        f"  Found {bcolors.BOLD}{len(available_files)}{bcolors.ENDC} available {file_type} files.\n"
    )

    print(f"Checking existing {file_type} file directory...")
    existing_files = os.listdir(output_directory)
    existing_files.sort()
    print(
        f"  Found {bcolors.BOLD}{len(existing_files)}{bcolors.ENDC} existing {file_type} files.\n"
    )

    print(f"Working out what files need downloading...")
    existing_files = set(existing_files)
    files_to_download = [file for file in available_files if file not in existing_files]
    print(
        f"  There are {bcolors.BOLD}{len(files_to_download)}{bcolors.ENDC} {file_type} files to download.\n"
    )

    if files_to_download:
        print(f"Commencing download of {file_type} files now...")
        download_file_list(output_directory, token, files_to_download)
    else:
        print(f"{bcolors.WARNING}No {file_type} files to download.{bcolors.ENDC}\n")

    return


def download_needed_wav(output_directory, deployment_directory, scenario_interval_dir, inclusion_radius, token):
    # Define exclusion range as an offset from the inclusion.
    exclusion_radius = 2000 + inclusion_radius

    # Instantiate ONC object.
    onc_api = ONC(token, timeout=600)

    # Get the desired filter object to query for files at ONC servers.
    filters = get_deployment_filters(deployment_directory, filter_type="WAV")

    print(f"Finding available WAV files from deployment...")
    available_files = []
    for new_filter in filters:
        try:
            files_dict = onc_api.getListByDevice(new_filter, allPages=True)
            available_files.extend(files_dict["files"])
        except Exception as e:
            print(f"  {bcolors.WARNING}Error when running for {new_filter['deviceCode']} ({new_filter['dateFrom']} - {new_filter['dateTo']}){bcolors.ENDC}")

    available_files.sort()

    interval_file_names = os.listdir(scenario_interval_dir)
    # Read vessel intervals range data from csv.
    vessel_interval_file_names = [file for file in interval_file_names if file.lower().endswith('unique_vessel_intervals.csv')]
    vessel_interval_data = pd.read_csv(os.path.join(scenario_interval_dir, vessel_interval_file_names[0]))

    # Get data from only the selected inclusion/exclusion range.
    vessel_data_from_range = vessel_interval_data[vessel_interval_data["exclusion_radius"] == exclusion_radius]

    # Get list of vessel files to download.
    vessel_to_download = find_in_range_wav(vessel_data_from_range, available_files)

    # Read background range data from csv.
    background_interval_file_names = [file for file in interval_file_names if file.lower().endswith('background_intervals.csv')]
    background_interval_data = pd.read_csv(os.path.join(scenario_interval_dir, background_interval_file_names[0]))

    # Get data from only the selected inclusion/exclusion range.
    background_data_from_range = background_interval_data[background_interval_data["exclusion_radius"] == exclusion_radius]

    # Get list of vessel files to download.
    background_to_download = find_in_range_wav(background_data_from_range, available_files)

    # We do not need more background files than vessel.
    to_download = vessel_to_download + background_to_download[:int(len(vessel_to_download)/2)]

    print(f"Checking existing WAV files in directory...")
    existing_files = os.listdir(output_directory)
    existing_files.sort()
    print(
        f"  Found {bcolors.BOLD}{len(existing_files)}{bcolors.ENDC} existing WAV files.\n"
    )

    print(f"Working out what files need downloading...")
    existing_files = set(existing_files)
    files_to_download = [file for file in to_download if file not in existing_files]
    print(
        f"  There are {bcolors.BOLD}{len(files_to_download)}{bcolors.ENDC} WAV files to download.\n"
    )

    if files_to_download:
        print(f"Commencing download of WAV files now...")
        download_file_list(output_directory, token, files_to_download)
    else:
        print(f"{bcolors.WARNING}No WAV files to download.{bcolors.ENDC}\n")
