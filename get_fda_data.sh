
# get list of files to be downloaded
curl -X GET 'https://api.fda.gov/download.json' | \
    cat - | \
    jq -r '.results.drug.event.partitions[].file' > files.txt

# get number of cores of the computer as cardinality of the files set is around 900
cores=$(cat /proc/cpuinfo | grep processor | wc -l)

# split file into cores chunks
# -d use numeral suffixes on files
# -n number of chunks
# l/N don't break lines
# outputs: f_0, f_1, ..., f_($cores - 1)
split -d -n l/$cores files.txt f_

# for each chunk

for fname in $(ls -1 f_*); do
    (for f in $(cat $fname); do 
        # wget
            # -c get partial file
            # -O - output to stdout instead of to file
        # jq
            # -r raw json, no escapes
            # @json = output in json format
        # uuidgen -r = create random uuid
        wget -c "$f" -O - | gunzip | jq -r '.results[]|@json' > $(uuidgen -r)"_file.json"; 
        done
    ) &
done

# wait for all processes to finish
wait
exit 0