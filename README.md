# OpenFDA Adverse events Pipeline

OpenTargets ETL pipeline to process OpenFDA FAERS DB. 

The openFDA drug adverse event API returns data that has been collected from the FDA Adverse Event Reporting System (FAERS), a database that contains information on adverse event and medication error reports submitted to FDA.

### Summary

1. openFDA FAERS data [download](https://open.fda.gov/apis/drug/event/download/) (~ 900 files) \n [Click here from more details](#Produce-the-raw-json-from-scratch)
 
2. Pre-processing of this data using [platformDataProcessFDA.sc](https://github.com/opentargets/platform-etl-openfda-faers/blob/master/platformDataProcessFDA.sc) scala script:
  - Filtering:
    - Only reports submitted by health professionals (*primarysource.qualification* in (1,2,3)).
    - Exclude reports that resulted in death (no entries with *seriousnessdeath*=1).  
    - Only drugs that were considered by the reporter to be the cause of the event (*drugcharacterization*=1).
    - Remove events (but not the whole report which might have multiple events) that are [blacklisted ](https://github.com/opentargets/platform-etl-openfda-faers/blob/master/blacklisted_events.txt)(blacklist curated manually to exclude events that are uninformative for our purposes).
  - Match FDA drug names to Open Targets drug names & then map all these back to their ChEMBL id:
    - Open Targets drug index fields:  *‘chembl_id’, ‘synonyms’, ‘pref_name’, ‘trade_names’*.
    - openFDA adverse event data fields: *‘drug.medicinalproduct’, ‘drug.openfda.generic_name’, ‘drug.openfda.brand_name’, ‘drug.openfda.substance_name’*.
  - Generate table where each row is a unique drug-event pair and count the number of report IDs for each pair, the total number of reports, the total number of reports per drug and the total number of reports per event. Using these calculate the fields required for estimating the significance of each event occuring for each drug, e.g. log-likelihood ratio, (llr) (based on [FDA LRT method](https://openfda.shinyapps.io/LRTest/_w_c5c2d04d/lrtmethod.pdf)).
3. Calculate significance of each event for all drugs based on the FDA LRT method (Monte Carlo simulation) using the [openFDA_MonteCarlo_drugs.R](https://github.com/opentargets/platform-etl-openfda-faers/blob/master/R/openFDA_MonteCarlo_drugs.R) script. 

### Requirements

1. OpenJDK 1.8
2. scala 2.12.x (through SDKMAN is simple)
3. ammonite REPL
4. Drug index dump from OpenTargets ES
5. OpenFDA FAERS DB

### Run the scala script

```sh
export JAVA_OPTS="-Xms512m -Xmx<mostofthememingigslike100G>"
# to compute the dataset
time amm platformDataProcessFDA.sc \
    --drugSetPath "/data/jsonl/19.06_drug-data.json" \
    --inputPathPrefix "/data/eirini/raw/**/*.jsonl" \
    --outputPathPrefix /data/eirini/out
```

### Generate the drug dump from ES7

You will need to either connect to a machine containing the ES or forward the ssh port from it
```sh
elasticdump --input=http://localhost:9200/19.06_drug-data \
    --output=19.06_drug-data.json \
    --type=data  \
    --limit 10000 \
    --sourceOnly
```

### Produce the raw json from scratch

In the case you may want to generate all data again even the raw data this is the
piece of bash scripts I used to produce it

```bash
curl -XGET 'https://api.fda.gov/download.json' | \
    cat - | \
    jq -r '.results.drug.event.partitions[].file' > files.txt

# get number of cores of the computer as cardinality of the files set is around 900
cores=$(cat /proc/cpuinfo | grep processor | wc -l)

# split file into cores chunks
split -d -n l/$cores files.txt f_

for fname in $(ls -1 f_*); do
    (for f in $(cat $fname); do wget -c "$f" -O - | gunzip | jq -r '.results[]|@json' > $(uuidgen -r)"_file.json"; done) &
done

# wait for all processes to finish
wait
exit 0
```

### Using S3 command (AWS)
```
pip install aws
```

Script for using AWS storage


```bash
curl -XGET 'https://api.fda.gov/download.json' | \
    cat - | \
    jq -r '.results.drug.event.partitions[].file' > files.txt

sed  's?https://download.open.fda.gov/?sudo aws s3 cp  s3://download.open.fda.gov/?' files.txt | sed 's?json.zip?json.zip `uuidgen -r`.json.zip --no-sign-request?' > go.sh

```

Run "go.sh" in the directory that you want to use and after unzip the files.

This is an example of the single command generated.
```
sudo aws s3 cp  s3://download.open.fda.gov/drug/event/2005q3/drug-event-0004-of-0005.json.zip `uuidgen -r`.json.zip --no-sign-request
````






### Montecarlo implementation for the critical value

Using the output of the previous run as input for this one as follows

```sh
export JAVA_OPTS="-Xms512m -Xmx<mostofthememingigslike100G>"
# to compute the dataset
time amm platformDataProcessFDAMonteCarlo.sc \
    --inputPath /data/jsonl/ \
    --outputPathPrefix /data/out \
    --permutations 1000 \
    --percentile 0.95
```

Former gist link to the **R** implementation `https://gist.github.com/mkarmona/101f6f5ce3befe0996966711e847f5f0`

# Copyright
Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
