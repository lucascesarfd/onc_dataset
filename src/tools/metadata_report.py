import pandas as pd
import matplotlib.pyplot as plt

def generate_report(metadata_file, output_dir):
    meta = pd.read_csv(metadata_file)
    classes = ["other", "passengership", "tug", "tanker", "cargo"]
    duration_dict = {}
    data_info = []

    print("Getting info from Metadata")
    for label in classes:
        aux = meta[meta["label"] == label]
        mmsi = aux["MMSI"].nunique()
        max_date = aux["date"].max()
        min_date = aux["date"].min()
        time = aux["duration_sec"].sum()
        timedelta = pd.to_timedelta(f"{time}s")
        duration_dict[label] = time
        data_info.append(f"Class {label}")
        data_info.append(f"  Period: from {min_date} to {max_date}")
        data_info.append(f"  Total Duration: {timedelta}")
        data_info.append(f"  Number of recordings: {len(aux)}")
        data_info.append(f"  Number of unique vessels: {mmsi}")
        data_info.append("")

    classes = duration_dict.keys()
    duration = duration_dict.values()

    print("Save into a figure")
    # Save the plot.
    plt.figure(figsize=[10, 6])
    plt.xlabel("Class")
    plt.ylabel("Duration (s)")
    plt.bar(classes, duration)
    plt.gca().yaxis.grid()
    plt.savefig(output_dir+"_duration.svg")
    plt.show()

    print("Save into a text file")
    # save info into txt file.
    with open(output_dir+"_info.txt", 'w') as f:
        for line in data_info:
            f.write(line)
            f.write('\n')

    print("Finished")

if __name__ == "__main__":

    metadata_file = "/workspaces/underwater/dataset/07_classified_wav_files/inclusion_4000_exclusion_6000/metadata.csv"
    output_dir = "/workspaces/underwater/dataset/07_classified_wav_files/inclusion_4000_exclusion_6000/report/metadata"
    generate_report(metadata_file, output_dir)
