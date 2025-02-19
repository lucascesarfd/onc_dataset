ONC_TOKEN=""
WORK_DIR="/workspaces/underwater/dataset"

# 0 - Query ONC deployments
# 1 - Download AIS files
# 2 - Parse AIS to JSON
# 3 - Clean AIS data
# 4 - Combine deployment AIS data
# 5 - Identify scenarios
# 6 - Download WAV files
# 7 - Classify WAV files from range
# 8 - Download CTD files
# 9 - Clean CTD files
# 10 - Generate the metadata for the full dataset
# 11 - Generate metadata for small periods of duration
# 12 - Split dataset into Train, Test and Validation
# 13 - Generate a balanced version of the full dataset
STEPS=[0,1,2,3,4,5,6,7,8,9,10]

MAX_INCLUSION_RADIUS=15000.0
INCLUSION_RADIUS=4000

METADATA_SECONDS=1
METADATA_FILE="metadata"
METADATA_VAL_SPLIT=0.2
METADATA_TEST_SPLIT=0.1

# Pacific - Salish Sea - Strait of Georgia - Fraser River Delta (49.080927,-123.338713)
AIS_CODE = "DIGITALYACHTAISNET1302-0097-01"
WAV_DEVICES = [{"deviceCode": "ICLISTENAF2523"}, {"deviceCode": "ICLISTENAF2556"}]
CTD_DEVICE = "SBECTD19p6935"

# Define if the metadata will include ctd information. Only needed for step 10.
USE_CTD=True
