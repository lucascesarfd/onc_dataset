import os
import time
import folium

import numpy as np
import pandas as pd

from utils import (
    create_dir,
    get_hydrophone_deployments,
    pandas_timestamp_to_zulu_format,
    dump_data_frame_to_feather_file,
    read_data_frame_from_feather_file,
)


class formating_str:
    DASHES = "  " + "-" * 59
    HEADER = ("Day (#)", "Elapsed (s)", "Completed (%)", "Estimated TTC (s)")
    HEADER_LAYOUT = "  {:^9s}|{:^13s}|{:^15s}|{:^19s}"
    DATA_LAYOUT = "  {:^9d}|{:^13.3f}|{:^15.2f}|{:^19.3f}"


def identify_scenarios(
    working_directory,
    deployment_directory,
    scenario_intervals_directory,
    interval_ais_data_directory,
    combined_deployment_directory,
):

    # Read in the hydrophone deployments as we will treat each deployment as an individual dataset.
    hydrophone_deployments = get_hydrophone_deployments(deployment_directory)

    # Find all of the cleaned AIS files for each deployment.
    devices = hydrophone_deployments.keys()
    for device in devices:
        for deployment in hydrophone_deployments[device].itertuples(index=False):

            deployment_begin = pd.Timestamp(deployment.begin).normalize()
            deployment_end = pd.Timestamp(deployment.end).normalize() + pd.DateOffset(
                days=1
            )
            deployment_duration = (deployment_end - deployment_begin) / np.timedelta64(
                24, "h"
            )

            days_processed = 0

            print(
                "\nWorking on deviceID {0} for deployment {1} to {2}...".format(
                    device, deployment_begin, deployment_end
                )
            )
            print(formating_str.DASHES)
            print(formating_str.HEADER_LAYOUT.format(*formating_str.HEADER))
            print(formating_str.DASHES)

            deployment_file_name = "_".join(
                [
                    device,
                    pandas_timestamp_to_zulu_format(deployment_begin),
                    pandas_timestamp_to_zulu_format(deployment_end),
                    "clean_interpolated_ais_data.feather",
                ]
            )

            data_frame = read_data_frame_from_feather_file(
                os.path.join(combined_deployment_directory, deployment_file_name)
            )

            minimum_distance = 1000
            maximum_distance = 10000
            distance_step = 1000
            inclusion_radii = np.arange(
                minimum_distance, maximum_distance + distance_step, distance_step
            )
            exclusion_radius_offset = 2000
            seconds_of_timeframe = 60

            data_frame = data_frame.sort_values(by=["pd_timestamp"])
            time_intervals = pd.interval_range(
                start=data_frame["pd_timestamp"].iloc[0].normalize(),
                end=data_frame["pd_timestamp"].iloc[-1].normalize()
                + pd.DateOffset(days=1),
                freq="{0}S".format(seconds_of_timeframe),
                closed="left",
            )

            # Find the time intervals where only one vessel is within range.
            exclusion_zone_colour = "#ad2727"
            inclusion_zone_colour = "#27ad27"

            map_data_directory = create_dir(
                working_directory, "99_inclusion_exclusion_zone_maps"
            )

            # Sanity checking for one-time map generation per entry.
            exclusion_radii_mapped = []
            inclusion_radii_mapped = []

            # Breaking the data off into dictionaries of DataFrames to make organising it easier, compared to n-columns being added to all entries.
            inclusion_exclusion_interval_dicts = {}
            background_noise_interval_dicts = {}

            data_frame.set_index(data_frame["pd_timestamp"], inplace=True)

            grouped_by_time_intervals = data_frame.groupby(
                pd.Grouper(freq="1Min", offset="0Min", label="left")
            )

            reporting_day = (
                grouped_by_time_intervals.first().iloc[0]["pd_timestamp"].normalize()
            )

            print(f"Processing day {reporting_day} now...")
            start_time = time.time()

            # Due to the size of the primary DataFrame, we only really want to iterate through the entire thing once, so the interval is top.
            for interval_left, interval_data in grouped_by_time_intervals:
                if interval_left.normalize() > reporting_day:
                    days_processed += 1

                    day = days_processed
                    elapsed = time.time() - start_time
                    completed = (days_processed / deployment_duration) * 100.0
                    forecast = (
                        (elapsed / days_processed) * deployment_duration
                    ) - elapsed

                    print(
                        formating_str.DATA_LAYOUT.format(
                            day, elapsed, completed, forecast
                        )
                    )
                    reporting_day = interval_left.normalize()

                for inclusion_radius in inclusion_radii:
                    exclusion_radius = inclusion_radius + exclusion_radius_offset

                    # No entries within exclusion range means that we can use this interval for background noise estimation.
                    if (
                        sum(interval_data["distance_to_hydrophone"] <= exclusion_radius)
                        == 0
                    ):
                        if exclusion_radius not in background_noise_interval_dicts:
                            background_noise_interval_dicts[exclusion_radius] = []

                        background_noise_interval_dicts[exclusion_radius].append(
                            interval_left
                        )

                        # Create the map for sanity checking.
                        if exclusion_radius not in exclusion_radii_mapped:
                            exclusion_radii_mapped.append(exclusion_radius)
                            folium_map = folium.Map(
                                location=(deployment.latitude, deployment.longitude),
                                tiles="CartoDB positron",
                                zoom_start=12,
                            )

                            folium.Circle(
                                location=(deployment.latitude, deployment.longitude),
                                radius=float(exclusion_radius),
                                dash_array="10,20",
                                color=exclusion_zone_colour,
                                fill_color=exclusion_zone_colour,
                                fill_opacity=0.2,
                                popup=f"Exclusion radius of {exclusion_radius} metres",
                                tooltip=f"Exclusion radius of {exclusion_radius} metres",
                            ).add_to(folium_map)

                            folium.Circle(
                                location=(deployment.latitude, deployment.longitude),
                                radius=1.0,
                                color="#3388ff",
                                popup=f"{device}",
                                tooltip=f"{device}",
                            ).add_to(folium_map)

                            folium_map.save(
                                os.path.join(
                                    map_data_directory,
                                    f"exclusion_radius_{exclusion_radius:05d}_metres.html",
                                )
                            )

                    # Else there is something in the exclusion range.
                    else:
                        only_one_vessel_within_exclusion_radius = (
                            interval_data[
                                interval_data["distance_to_hydrophone"]
                                <= exclusion_radius
                            ]["mmsi"]
                            .unique()
                            .shape[0]
                        ) == 1

                        number_of_messages_in_inclusion_radius = sum(
                            interval_data["distance_to_hydrophone"] <= inclusion_radius
                        )
                        number_of_messages_in_exclusion_radius = sum(
                            interval_data["distance_to_hydrophone"] <= exclusion_radius
                        )
                        all_messages_are_within_inclusion_radius = (
                            number_of_messages_in_inclusion_radius
                            == number_of_messages_in_exclusion_radius
                        )

                        # If there is only a single vessel within the exclusion range and that vessel is within the inclusion range.
                        if (
                            only_one_vessel_within_exclusion_radius
                            and all_messages_are_within_inclusion_radius
                        ):
                            scenario = (
                                f"in_{inclusion_radius:05d}_out_{exclusion_radius:05d}"
                            )

                            if scenario not in inclusion_exclusion_interval_dicts:
                                inclusion_exclusion_interval_dicts[scenario] = []

                            inclusion_exclusion_interval_dicts[scenario].append(
                                interval_left
                            )

                            if inclusion_radius not in inclusion_radii_mapped:
                                inclusion_radii_mapped.append(inclusion_radius)

                                folium_map = folium.Map(
                                    location=(
                                        deployment.latitude,
                                        deployment.longitude,
                                    ),
                                    tiles="CartoDB positron",
                                    zoom_start=12,
                                )

                                folium.Circle(
                                    location=(
                                        deployment.latitude,
                                        deployment.longitude,
                                    ),
                                    radius=float(exclusion_radius),
                                    dash_array="10,20",
                                    color=exclusion_zone_colour,
                                    fill_color=exclusion_zone_colour,
                                    fill_opacity=0.2,
                                    popup=f"Exclusion radius of {exclusion_radius} metres",
                                    tooltip=f"Exclusion radius of {exclusion_radius} metres",
                                ).add_to(folium_map)

                                folium.Circle(
                                    location=(
                                        deployment.latitude,
                                        deployment.longitude,
                                    ),
                                    radius=float(inclusion_radius),
                                    dash_array="10,20",
                                    color=inclusion_zone_colour,
                                    fill_color=inclusion_zone_colour,
                                    fill_opacity=0.2,
                                    popup=f"Inclusion radius of {inclusion_radius} metres",
                                    tooltip=f"Inclusion radius of {inclusion_radius} metres",
                                ).add_to(folium_map)

                                folium.Circle(
                                    location=(
                                        deployment.latitude,
                                        deployment.longitude,
                                    ),
                                    radius=1.0,
                                    color="#3388ff",
                                    popup=f"{device}",
                                    tooltip=f"{device}",
                                ).add_to(folium_map)

                                folium_map.save(
                                    os.path.join(
                                        map_data_directory,
                                        f"inclusion_radius_{inclusion_radius:05d}_metres_exclusion_radius_{exclusion_radius:05d}_metres.html",
                                    )
                                )

            print(f"Finished identifying all scenarios...")

            # Find unique background intervals.
            start_time = time.time()
            unique_background_intervals = pd.DataFrame()

            list_of_data_to_fetch = {}

            # We need to incrementally enforce uniqueness so that each sample is statistically isolated.
            # That is to say, if an interval is selected at 10000 meters, it needs to be removed from all closer ranges.
            descending_keys = list(background_noise_interval_dicts.keys())
            descending_keys.sort(reverse=True)

            for distance_index, distance_key in enumerate(descending_keys):

                print(
                    f"Identifying time intervals that are consecutively large enough..."
                )
                start_time = time.time()
                minimum_consecutive_minutes = 30

                # Create a temporary DataFrame with a single column.
                temporary_data_frame = pd.DataFrame(
                    background_noise_interval_dicts[distance_key],
                    columns=["closed_left"],
                )

                # Calculate the difference between each row and the next.
                temporary_data_frame["difference"] = (
                    temporary_data_frame["closed_left"]
                    .diff()
                    .dt.total_seconds()
                    .div(60, fill_value=0.0)
                )

                # Tally the number of consecutive differences (e.g. where delta_time == 1 consistently).
                temporary_data_frame["consecutive"] = (
                    temporary_data_frame["difference"]
                    .groupby(
                        (
                            temporary_data_frame["difference"]
                            != temporary_data_frame["difference"].shift()
                        ).cumsum()
                    )
                    .transform("size")
                )

                # Pull only the intervals where the consecutive number (e.g. total time) meets the criteria above.
                temporary_data_frame = temporary_data_frame[
                    temporary_data_frame["consecutive"] >= minimum_consecutive_minutes
                ]

                start_index = -1
                end_index = -1
                start_timestamp = None
                end_timestamp = None

                # There is most certainly a more pandas way of doing this whole process, but bugger it, this is fast enough.
                while end_index != (temporary_data_frame.shape[0] - 1):

                    start_index = end_index + 1
                    end_index = (
                        start_index
                        + temporary_data_frame.iloc[start_index]["consecutive"]
                        - 1
                    )
                    start_timestamp = temporary_data_frame.iloc[start_index][
                        "closed_left"
                    ]
                    end_timestamp = temporary_data_frame.iloc[end_index][
                        "closed_left"
                    ] + pd.DateOffset(minutes=1)
                    duration_seconds = (
                        end_timestamp - start_timestamp
                    ) / np.timedelta64(1, "s")

                    if distance_key not in list_of_data_to_fetch:
                        list_of_data_to_fetch[distance_key] = []

                    list_of_data_to_fetch[distance_key].append(
                        [start_timestamp, end_timestamp]
                    )

                start_time = time.time()

                # Having the reference list as a set makes the look-up an O(1) operation instead of an O(N) for a list.
                # Utterly hilarious difference in performance between those two with large lists.
                reference_list = set(temporary_data_frame["closed_left"])

                for modify_index in range(distance_index + 1, len(descending_keys)):
                    background_noise_interval_dicts[descending_keys[modify_index]] = [
                        value
                        for value in background_noise_interval_dicts[
                            descending_keys[modify_index]
                        ]
                        if value not in reference_list
                    ]

            file_name = "_".join(
                [
                    device,
                    pandas_timestamp_to_zulu_format(deployment_begin),
                    pandas_timestamp_to_zulu_format(deployment_end),
                    "exclusion_intervals.csv",
                ]
            )

            output_file = open(
                os.path.join(scenario_intervals_directory, file_name), "w"
            )
            output_file.write("exclusion_radius,begin,end\n")

            for distance_key, intervals in list_of_data_to_fetch.items():

                for interval in intervals:

                    output_file.write(
                        ",".join(
                            [
                                f"{distance_key:05d}",
                                pandas_timestamp_to_zulu_format(interval[0]),
                                pandas_timestamp_to_zulu_format(interval[1]),
                            ]
                        )
                        + "\n"
                    )

                    # Dump the AIS data for these scenarios to individual feather files.
                    feather_file_name = "_".join(
                        [
                            pandas_timestamp_to_zulu_format(interval[0]),
                            pandas_timestamp_to_zulu_format(interval[1]),
                            "interval_data.feather",
                        ]
                    )

                    dump_data_frame_to_feather_file(
                        os.path.join(interval_ais_data_directory, feather_file_name),
                        data_frame[
                            (
                                data_frame["pd_timestamp"]
                                >= (interval[0] - pd.DateOffset(minutes=1))
                            )
                            & (
                                data_frame["pd_timestamp"]
                                <= (interval[1] + pd.DateOffset(minutes=1))
                            )
                        ],
                    )

            output_file.close()

            # Find unique inclusion intervals.
            start_time = time.time()
            unique_inclusion_intervals = pd.DataFrame()

            list_of_data_to_fetch = {}

            # We need to incrementally enforce uniqueness so that each sample is statistically isolated.
            # That is to say, if an interval is selected at 10000 meters, it needs to be removed from all closer ranges.
            descending_keys = list(inclusion_exclusion_interval_dicts.keys())
            descending_keys.sort(reverse=True)

            for distance_index, scenario_key in enumerate(descending_keys):
                start_time = time.time()
                minimum_consecutive_minutes = 5

                # Create a temporary DataFrame with a single column.
                temporary_data_frame = pd.DataFrame(
                    inclusion_exclusion_interval_dicts[scenario_key],
                    columns=["closed_left"],
                )

                # Calculate the difference between each row and the next.
                temporary_data_frame["difference"] = (
                    temporary_data_frame["closed_left"]
                    .diff()
                    .dt.total_seconds()
                    .div(60, fill_value=0.0)
                )

                # Tally the number of consecutive differences (e.g. where delta_time == 1 consistently).
                temporary_data_frame["consecutive"] = (
                    temporary_data_frame["difference"]
                    .groupby(
                        (
                            temporary_data_frame["difference"]
                            != temporary_data_frame["difference"].shift()
                        ).cumsum()
                    )
                    .transform("size")
                )

                # Pull only the intervals where the consecutive number (e.g. total time) meets the criteria above.
                temporary_data_frame = temporary_data_frame[
                    temporary_data_frame["consecutive"] >= minimum_consecutive_minutes
                ]

                start_index = -1
                end_index = -1
                start_timestamp = None
                end_timestamp = None

                # There is most certainly a more pandas way of doing this whole process, but bugger it, this is fast enough.
                while end_index != (temporary_data_frame.shape[0] - 1):

                    start_index = end_index + 1
                    end_index = (
                        start_index
                        + temporary_data_frame.iloc[start_index]["consecutive"]
                        - 1
                    )
                    start_timestamp = temporary_data_frame.iloc[start_index][
                        "closed_left"
                    ]
                    end_timestamp = temporary_data_frame.iloc[end_index][
                        "closed_left"
                    ] + pd.DateOffset(minutes=1)
                    duration_seconds = (
                        end_timestamp - start_timestamp
                    ) / np.timedelta64(1, "s")

                    if scenario_key not in list_of_data_to_fetch:
                        list_of_data_to_fetch[scenario_key] = []

                    list_of_data_to_fetch[scenario_key].append(
                        [start_timestamp, end_timestamp]
                    )

                # Having the reference list as a set makes the look-up an O(1) operation instead of an O(N) for a list.
                # Utterly hilarious difference in performance between those two with large lists.
                reference_list = set(temporary_data_frame["closed_left"])

                for modify_index in range(distance_index + 1, len(descending_keys)):
                    inclusion_exclusion_interval_dicts[
                        descending_keys[modify_index]
                    ] = [
                        value
                        for value in inclusion_exclusion_interval_dicts[
                            descending_keys[modify_index]
                        ]
                        if value not in reference_list
                    ]

            file_name = "_".join(
                [
                    device,
                    pandas_timestamp_to_zulu_format(deployment_begin),
                    pandas_timestamp_to_zulu_format(deployment_end),
                    "inclusion_intervals.csv",
                ]
            )

            output_file = open(
                os.path.join(scenario_intervals_directory, file_name), "w"
            )
            output_file.write("inclusion_radius,exclusion_radius,begin,end\n")

            for scenario_key, intervals in list_of_data_to_fetch.items():
                for interval in intervals:
                    scenario_parts = scenario_key.split("_")
                    output_file.write(
                        ",".join(
                            [
                                f"{scenario_parts[1]}",
                                f"{scenario_parts[3]}",
                                pandas_timestamp_to_zulu_format(interval[0]),
                                pandas_timestamp_to_zulu_format(interval[1]),
                            ]
                        )
                        + "\n"
                    )

                    # Dump the AIS data for these scenarios to individual feather files.
                    feather_file_name = "_".join(
                        [
                            pandas_timestamp_to_zulu_format(interval[0]),
                            pandas_timestamp_to_zulu_format(interval[1]),
                            "interval_data.feather",
                        ]
                    )

                    dump_data_frame_to_feather_file(
                        os.path.join(interval_ais_data_directory, feather_file_name),
                        data_frame[
                            (
                                data_frame["pd_timestamp"]
                                >= (interval[0] - pd.DateOffset(minutes=1))
                            )
                            & (
                                data_frame["pd_timestamp"]
                                <= (interval[1] + pd.DateOffset(minutes=1))
                            )
                        ],
                    )

            output_file.close()
