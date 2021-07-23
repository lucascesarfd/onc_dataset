import argparse

from utils import bcolors, create_dir
from download import query_onc_deployments, download_files
from parse import parse_ais_to_json
from clean import clean_ais_data
from combine import combine_deployment_ais_data
from identify import identify_scenarios


def create_parser():
    # Create the parser
    parser = argparse.ArgumentParser(description="Dataset Preparation Script")

    # Add the arguments
    parser.add_argument(
        "--work_dir",
        "-w",
        type=str,
        default="/workspaces/underwater/dataset",
        help="The path to the workspace directory.",
    )

    parser.add_argument(
        "onc_token",
        type=str,
        default="",
        help="The Ocean Networks Canada Token to download the files."
        " It can be obtained at https://wiki.oceannetworks.ca/display/O2KB/Get+your+API+token.",
    )

    parser.add_argument(
        "--steps",
        "-s",
        action='store',
        type=int,
        nargs="+",
        default=[0, 1, 2, 3, 4, 5, 6],
        help="The numbers related to the steps that you want to execute. "
        "By default, all tests are executed."
        "0 - Query ONC deployments; "
        "1 - Download AIS files; "
        "2 - Download WAV files; "
        "3 - Parse AIS to JSON; "
        "4 - Clean AIS data; "
        "5 - Combine deployment AIS data; "
        "6 - Identify scenarios.",
    )

    return parser


def _main():
    # Execute the parse_args() method
    parser = create_parser()
    args = parser.parse_args()

    print(f"{bcolors.HEADER}Dataset Preparation Script{bcolors.ENDC}\n")

    working_directory = args.work_dir

    deployment_directory = create_dir(working_directory, "00_hydrophone_deployments")
    raw_ais_directory = create_dir(working_directory, "01_raw_ais_files")
    raw_wav_directory = create_dir(working_directory, "02_raw_wav_files")
    parsed_ais_directory = create_dir(working_directory, "03_parsed_ais_files")
    clean_ais_directory = create_dir(working_directory, "04_clean_and_inrange_ais_data")
    combined_deployment_directory = create_dir(working_directory, "05_combined_deployment_ais_data")
    scenario_intervals_directory = create_dir(working_directory, "06a_scenario_intervals")
    interval_ais_data_directory = create_dir(working_directory, "06b_interval_ais_data")

    #token = "155f45d8-30ec-4e7f-a866-167c7d424635"
    token = args.onc_token

    # The maximum distance (metres) that a vessel can be from the hydrophone before we start caring about it.
    inclusion_radius = 15000.0

    if 0 in args.steps:
        print(f"{bcolors.HEADER}Querying Ocean Natworks Canada for Deployments{bcolors.ENDC}")
        query_onc_deployments(
            deployment_directory,
            token,
        )

    if 1 in args.steps:
        print(f"{bcolors.HEADER}Downloading AIS Files{bcolors.ENDC}")
        download_files(
            raw_ais_directory,
            deployment_directory,
            token,
            file_type="AIS",
        )

    if 2 in args.steps:
        print(f"{bcolors.HEADER}Downloading Raw WAV Files{bcolors.ENDC}")
        download_files(
            raw_wav_directory,
            deployment_directory,
            token,
            file_type="WAV",
        )

    if 3 in args.steps:
        print(f"{bcolors.HEADER}Parsing AIS files to JSON files{bcolors.ENDC}")
        parse_ais_to_json(
            raw_ais_directory,
            parsed_ais_directory,
            single_threaded_processing=True,
        )

    if 4 in args.steps:
        print(f"{bcolors.HEADER}Cleaning AIS data{bcolors.ENDC}")
        clean_ais_data(
            deployment_directory,
            parsed_ais_directory,
            clean_ais_directory,
            _inclusion_radius=inclusion_radius,
            use_all_threads=False,
        )

    if 5 in args.steps:
        print(f"{bcolors.HEADER}Combining Deployment AIS data{bcolors.ENDC}")
        # This will run the shortest hydrophone deployment to speed up development.
        run_shortest = False
        combine_deployment_ais_data(
            deployment_directory,
            clean_ais_directory,
            combined_deployment_directory,
            run_shortest,
            inclusion_radius,
            use_all_threads=False,
        )

    if 6 in args.steps:
        print(f"{bcolors.HEADER}Identifying scenarios{bcolors.ENDC}")
        identify_scenarios(
            working_directory,
            deployment_directory,
            scenario_intervals_directory,
            interval_ais_data_directory,
            combined_deployment_directory,
        )


if __name__ == "__main__":
    _main()
