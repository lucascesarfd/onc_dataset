ONC_TOKEN=""
WORK_DIR="/workspaces/underwater/dataset"

# 0 - Query ONC deployments
# 1 - Download AIS files
# 2 - Download WAV files
# 3 - Parse AIS to JSON
# 4 - Clean AIS data
# 5 - Combine deployment AIS data
# 6 - Identify scenarios
# 7 - Classify WAV files from range
# 8 - Download CTD files
# 9 - Clean CTD files
# 10 - Generate the metadata for the full dataset
# 11 - Generate metadata for small periods of duration
# 12 - Split dataset into Train, Test and Validation
# 13 - Generate a balanced version of the full dataset
STEPS=[11, 12, 13]

MAX_INCLUSION_RADIUS=15000.0
INCLUSION_RADIUS=4000

METADATA_SECONDS=1
METADATA_FILE="metadata"
METADATA_VAL_SPLIT=0.2
METADATA_TEST_SPLIT=0.1