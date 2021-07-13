# Download OpenFDA data from Mac OS

# Environment
folder_root=$(realpath tmp/openfda)
folder_data="$folder_root/data"
folder_downloads="$folder_root/downloads"

# Prepare folders
mkdir -p "$folder_data"
mkdir -p "$folder_downloads"

# get list of files to be downloaded
cd "$folder_root"

echo "[FOLDER] Root folder: $folder_root"
echo "[FOLDER] Data folder: $folder_data"
echo "[FOLDER] Download folder: $folder_downloads"

echo "[PREPARE] Get file listing"
curl -X GET 'https://api.fda.gov/download.json' | \
    cat - | \
    jq -r '.results.drug.event.partitions[].file' > download_file_listing.txt

# get number of cores of the computer as cardinality of the files set is around 900
ncores=$(sysctl hw.ncpu | cut -f2 -d' ')
files_per_core=$(expr `cat download_file_listing.txt | wc -l` / $ncores)
echo "[PREPARE] Splitting download files among $ncores cores in the system, at $files_per_core files per core"

# split file into cores chunks
split -a 3 -l $files_per_core download_file_listing.txt download_partial_list.

# for each chunk
for fname in $(ls -1 download_partial_list.*); do
  echo "[DOWNLOAD] File listing '$fname'"
  (while IFS= read -r f; do
    # wget
        # -c get partial file
        # -O - output to stdout instead of to file
    # jq
        # -r raw json, no escapes
        # @json = output in json format
    # uuidgen = create random uuid
    wget -c -qO- "$f" | tar xzO | jq -r '.results[]|@json' > "$folder_data"/$(uuidgen | tr '[:upper:]' '[:lower:]')"_file.jsonl";
    #echo "[DEBUG] Download file: $f to " "$folder_data"/$(uuidgen | tr '[:upper:]' '[:lower:]')"_file.jsonl"
   done < "$fname"
  ) &
done

# wait for all processes to finish
wait
exit 0
