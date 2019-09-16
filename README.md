# OpenFDA Adverse events Pipeline

OpenTargets ETL pipeline to process OpenFDA FAERS DB. 

The openFDA drug adverse event API returns data that has been collected from the FDA Adverse Event Reporting System (FAERS), a database that contains information on adverse event and medication error reports submitted to FDA.

### Requirements

1. Java 1.8
2. https://github.com/YotpoLtd/metorikku
3. drug index dump from our internal ES image
4. OpenFDA FAERS drug files downloaded

### Run the scala script

In order to run the script you might want to update the input path entries from openfda.yaml file.

```sh
export JAVA_OPTS="-Xms512m -Xmx<mostofthememingigslike100G>"
# to compute the dataset
java -Dspark.master=local[*] -Dspark.sql.crossJoin.enabled=true -Dspark.driver.maxResultSize=0 -cp metorikku-standalone.jar com.yotpo.metorikku.Metorikku -c openfda.yaml
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

#### Produce the raw json from scratch

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
    (for f in $(cat $fname); do wget -c "$f" -O - | gunzip > $(uuidgen -r)"_file.json"; done) &
done

# wait for all processes to finish
wait
exit 0
```

### Montecarlo implementation for the critical value

Here the gist link to the **R** implementation `https://gist.github.com/mkarmona/101f6f5ce3befe0996966711e847f5f0`

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
