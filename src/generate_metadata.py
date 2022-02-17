import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from pydub.utils import mediainfo
from utils import read_data_frame_from_feather_file, get_min_max_normalization, get_min_max_values_from_df


def get_class_from_code(code):
    if code == 0:
        return "background"
    elif code == 52:
        return "tug"
    elif code >= 60 and code <= 69:
        return "passengership"
    elif code >= 70 and code <= 79:
        return "cargo"
    elif code >= 80 and code <= 89:
        return "tanker"
    
    return "other"


def get_mean_ctd_from_range(data_frame, begin_time, end_time):
    columns = ["t1", "c1", "p1", "sal", "sv"]

    ctd_df = data_frame[data_frame['date'].between(begin_time, end_time, inclusive="both")]
    ctd_df = ctd_df[columns]
    t1, c1, p1, sal, sv = ctd_df.apply(pd.to_numeric).mean()

    return t1, c1, p1, sal, sv


def get_full_ctd_dataframe(clean_ctd_directory):
    data_files = [file for file in os.listdir(clean_ctd_directory)]

    files = [
        read_data_frame_from_feather_file(
            os.path.join(clean_ctd_directory, file)
        )
        for file in data_files
    ]

    df = pd.concat(files)
    df.sort_values(by=['date'], ignore_index=True, inplace=True)

    return df


def generate_full_metadata(root_path, clean_ctd_directory, interval_ais_dir, inclusion_radius):

    columns = ["label", "duration_sec", "path", "sample_rate", "class_code",
               "date", "MMSI", "t1", "c1", "p1", "sal", "sv", "t1_norm",
               "c1_norm", "p1_norm", "sal_norm", "sv_norm"]

    metadata = {key:[] for key in columns}

    dir_vessel = os.path.join(root_path, "vessel")
    meta_vessel = os.path.join(dir_vessel, "intervals.csv")
    df_vessel = pd.read_csv(meta_vessel)

    ctd_df = get_full_ctd_dataframe(clean_ctd_directory)
    min_max_ctd = get_min_max_values_from_df(ctd_df, ["t1", "c1", "p1", "sal", "sv"])

    print(f"Vessel Metafile")
    for _, row in tqdm(df_vessel.iterrows(), total=df_vessel.shape[0]):
        begin_time = row["begin"].replace("-","").replace(":","")
        end_time = row["end"].replace("-","").replace(":","")

        interval_file = os.path.join(interval_ais_dir, f"{begin_time}_{end_time}_interval_data.feather")    
        metadata_file = read_data_frame_from_feather_file(interval_file)

        class_code = metadata_file[metadata_file["distance_to_hydrophone"] <= inclusion_radius].type_and_cargo.unique()[0]
        mmsi = metadata_file[metadata_file["distance_to_hydrophone"] <= inclusion_radius].mmsi.unique()[0]
        path = os.path.join(dir_vessel, f'{row["wav_file"]}.wav')
        info = mediainfo(path)

        t1, c1, p1, sal, sv = get_mean_ctd_from_range(ctd_df, begin_time, end_time)

        # Append AIS data
        metadata["class_code"].append(class_code)
        metadata["MMSI"].append(mmsi)
        metadata["path"].append(path)
        metadata["date"].append(row["begin"].replace("-","").split("T")[0])
        metadata["duration_sec"].append(info["duration"])
        metadata["sample_rate"].append(info["sample_rate"])
        metadata["label"].append(get_class_from_code(class_code))

        # Append CTD data
        metadata["t1"].append(t1)
        metadata["c1"].append(c1)
        metadata["p1"].append(p1)
        metadata["sal"].append(sal)
        metadata["sv"].append(sv)

        # Append normalized CTD data
        metadata["t1_norm"].append(get_min_max_normalization(t1, min_max_ctd["t1"][0], min_max_ctd["t1"][1]))
        metadata["c1_norm"].append(get_min_max_normalization(c1, min_max_ctd["c1"][0], min_max_ctd["c1"][1]))
        metadata["p1_norm"].append(get_min_max_normalization(p1, min_max_ctd["p1"][0], min_max_ctd["p1"][1]))
        metadata["sal_norm"].append(get_min_max_normalization(sal, min_max_ctd["sal"][0], min_max_ctd["sal"][1]))
        metadata["sv_norm"].append(get_min_max_normalization(sv, min_max_ctd["sv"][0], min_max_ctd["sv"][1]))

    dir_background = os.path.join(root_path, "background")
    meta_backgorund = os.path.join(dir_background, "intervals.csv")
    df_background = pd.read_csv(meta_backgorund)

    print(f"Background Metafile")
    for _, row in tqdm(df_background.iterrows(), total=df_background.shape[0]):
        begin_time = row["begin"].replace("-","").replace(":","")
        end_time = row["end"].replace("-","").replace(":","")

        interval_file = os.path.join(interval_ais_dir, f"{begin_time}_{end_time}_interval_data.feather")    
        metadata_file = read_data_frame_from_feather_file(interval_file)

        path = os.path.join(dir_background, f'{row["wav_file"]}.wav')
        info = mediainfo(path)

        t1, c1, p1, sal, sv = get_mean_ctd_from_range(ctd_df, begin_time, end_time)

        # Append AIS data
        class_code = 0
        metadata["class_code"].append(class_code)
        metadata["MMSI"].append(0)
        metadata["path"].append(path)
        metadata["date"].append(row["begin"].replace("-","").split("T")[0])
        metadata["duration_sec"].append(info["duration"])
        metadata["sample_rate"].append(info["sample_rate"])
        metadata["label"].append(get_class_from_code(class_code))

        # Append CTD data
        metadata["t1"].append(t1)
        metadata["c1"].append(c1)
        metadata["p1"].append(p1)
        metadata["sal"].append(sal)
        metadata["sv"].append(sv)

        # Append normalized CTD data
        metadata["t1_norm"].append(get_min_max_normalization(t1, min_max_ctd["t1"][0], min_max_ctd["t1"][1]))
        metadata["c1_norm"].append(get_min_max_normalization(c1, min_max_ctd["c1"][0], min_max_ctd["c1"][1]))
        metadata["p1_norm"].append(get_min_max_normalization(p1, min_max_ctd["p1"][0], min_max_ctd["p1"][1]))
        metadata["sal_norm"].append(get_min_max_normalization(sal, min_max_ctd["sal"][0], min_max_ctd["sal"][1]))
        metadata["sv_norm"].append(get_min_max_normalization(sv, min_max_ctd["sv"][0], min_max_ctd["sv"][1]))

    final_metadata = pd.DataFrame.from_dict(metadata)
    final_metadata.to_csv(os.path.join(root_path, "metadata.csv"), index=False)


def generate_balanced_metadata(metadata_file, root_path):
    classes = ["other", "passengership", "tug", "tanker", "cargo"]

    meta = pd.read_csv(os.path.join(root_path, metadata_file))
    meta_dict = {label:meta[meta["label"] == label]["duration_sec"].sum() for label in classes}
    min_time_label = min(meta_dict, key=meta_dict.get)
    idx_dict = {}
    for label in classes:
        base_time = meta_dict[min_time_label]
        idx_dict[label] = []
        for idx, row in meta[meta["label"] == label].iterrows():
            idx_dict[label].append(idx)
            base_time = base_time - row.duration_sec
            if base_time <= 0:
                break
    idx_list = [idx for _, sublist in idx_dict.items() for idx in sublist]
    file_name = metadata_file.split(".")[0]
    meta[meta.index.isin(idx_list)].to_csv(os.path.join(root_path, f"{file_name}_balanced.csv"), index=False)


def get_metadata_for_small_times(root_path, metadata_file, seconds):
    initial_meta = pd.read_csv(os.path.join(root_path, metadata_file))
    colunas = list(initial_meta.columns) + ['sub_init']
    meta = pd.DataFrame([], columns=colunas)

    size = 0
    for index, row in tqdm(initial_meta.iterrows(), total=initial_meta.shape[0]):
        duration = int(row.duration_sec)
        step = seconds
        for i in range(0, duration, step):
            meta.loc[size] = list(row) + [i]
            size += 1 

    file_name = metadata_file.split(".")[0]
    meta.to_csv(os.path.join(root_path, f"{file_name}_{seconds}s.csv"), index=False)


def split_dataset(root_path, metadata_file, validation_split=0.2, test_split=0.1, random_seed=42):
    metadata = pd.read_csv(os.path.join(root_path, metadata_file))
    metadata = metadata.sample(frac=1, random_state=random_seed).reset_index(drop=True)

    # Creating data indices for training and validation splits:
    dataset_size = len(metadata.index)
    test_idx = int(np.floor(test_split * dataset_size))
    validation_idx = test_idx + int(np.floor(validation_split * dataset_size))

    test_dataset = metadata[:test_idx]
    val_dataset = metadata[test_idx:validation_idx]
    train_dataset = metadata[validation_idx:]

    file_name = metadata_file.split(".")[0]

    test_dataset.to_csv(os.path.join(root_path, f"{file_name}_test.csv"), index=False)
    val_dataset.to_csv(os.path.join(root_path, f"{file_name}_validation.csv"), index=False)
    train_dataset.to_csv(os.path.join(root_path, f"{file_name}_train.csv"), index=False)


def main():
    print(f"Saving metadata into CSV")

    # 1 - Generate the metadata for the full dataset.
    print(f"Generate the metadata for the full dataset")
    inclusion_radius = 3000
    root_path = "/workspaces/underwater/dataset/07_classified_wav_files/inclusion_3000_exclusion_5000"
    interval_ais_dir = "/workspaces/underwater/dataset/06b_interval_ais_data/"
    clean_ctd_directory = "/workspaces/underwater/dataset/09_cleaned_ctd_files"
    generate_full_metadata(root_path, clean_ctd_directory, interval_ais_dir, inclusion_radius)

    # 2 - Split dataset into small periods of time.
    print(f"Split dataset into small periods of time")
    seconds = 10
    metadata_file = os.path.join(root_path, f"metadata.csv")
    get_metadata_for_small_times(root_path, metadata_file, seconds)

    # 3 - Split the original data into train, test and validation datasets.
    print(f"Split dataset into train, test and validation datasets")
    validation_split = 0.2
    test_split = 0.1
    metadata_file_sec = os.path.join(root_path, f"metadata_{seconds}s.csv")
    split_dataset(root_path, metadata_file_sec, validation_split=validation_split, test_split=test_split)


if __name__ == "__main__":
    main()