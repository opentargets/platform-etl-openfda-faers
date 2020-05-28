# OpenFDA Adverse events Pipeline

OpenTargets ETL pipeline to process OpenFDA FAERS DB. 

The openFDA drug adverse event API returns data that has been collected from the FDA Adverse Event Reporting System (FAERS), a database that contains information on adverse event and medication error reports submitted to FDA.

This project should be run as a Spark job to generate aggregate outputs of adverse drug events.

### Summary

1. Download the OpenFDA "FAERS" [data](https://open.fda.gov/apis/drug/event/download/) (~ 1000 files - May 2020)
 
2. __Stage 1:__ Pre-processing of this data (OpenFdaEtl.scala):
     - Filtering:
        - Only reports submitted by health professionals (*primarysource.qualification* in (1,2,3)).
        - Exclude reports that resulted in death (no entries with *seriousnessdeath*=1).  
        - Only drugs that were considered by the reporter to be the cause of the event (*drugcharacterization*=1).
        - Remove events (but not the whole report which might have multiple events) that are [blacklisted ](https://github.com/opentargets/platform-etl-openfda-faers/blob/master/blacklisted_events.txt) (see [Blacklist](#blacklist)).
    - Match FDA drug names to Open Targets drug names & then map all these back to their ChEMBL id:
        - Open Targets drug index fields:  *‘chembl_id’, ‘synonyms’, ‘pref_name’, ‘trade_names’*.
        - openFDA adverse event data fields: *‘drug.medicinalproduct’, ‘drug.openfda.generic_name’, ‘drug.openfda.brand_name’, ‘drug.openfda.substance_name’*.
    - Generate table where each row is a unique drug-event pair and count the number of report IDs for each pair, the total number of reports, the total number of reports per drug and the total number of reports per event. Using these calculate the fields required for estimating the significance of each event occuring for each drug, e.g. log-likelihood ratio, (llr) (based on [FDA LRT method](https://openfda.shinyapps.io/LRTest/_w_c5c2d04d/lrtmethod.pdf)).
3. __Stage 2:__ Calculate significance of each event for all drugs based on the FDA LRT method (Monte Carlo simulation) (MonteCarloSampling.scala). 

### Requirements

1. OpenJDK 1.8
2. scala 2.12.x (through SDKMAN is simple)
3. ammonite REPL
4. [Drug index dump from OpenTargets ES](#generate-the-indices-dump-from-es7)
5. [OpenFDA FAERS DB](#produce-the-raw-json-from-scratch)
6. Text file of [blacklisted events](#blacklist)

### Create a fat JAR
Simply run the following command:

```bash
sbt assembly
```
The jar will be generated under _target/scala-2.12.10/_

### Configuration

The base configuration is found under `src/main/resources/reference.conf`. If you want to use specific configurations
for a Spark job see [below](#load-with-custom-configuration). 

The `fda` section is specifically relevant to this pipeline. 

#### Montecarlo

Specify the number of permutations and the relevance percentile threshhold.

#### Fda Inputs

Paths to the blacklisted_events, chembl data and fda database dump. 

#### Outputs

Specify format in which output should be saved. If no outputs are specified the 
results will not be saved. 

Output can be either "csv" or "json".

### Running

#### Dataproc

##### Create cluster and launch

Here how to create a cluster using `gcloud` tool

```sh
gcloud beta dataproc clusters create \
    etl-cluster \
    --image-version=1.5-debian10 \
    --properties=yarn:yarn.nodemanager.vmem-check-enabled=false,spark:spark.debug.maxToStringFields=1024,spark:spark.master=yarn \
    --master-machine-type=n1-highmem-16 \
    --master-boot-disk-size=500 \
    --num-secondary-workers=0 \
    --worker-machine-type=n1-standard-16 \
    --num-workers=2 \
    --worker-boot-disk-size=500 \
    --zone=europe-west1-d \
    --project=open-targets-eu-dev \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --max-idle=30m
```

##### Submitting a job to existing cluster

And to submit the job with either a local jar or from a GCS Bucket (gs://...)

```sh
gcloud dataproc jobs submit spark \
           --cluster=etl-cluster \
           --project=open-targets-eu-dev \
           --region=europe-west1 \
           --async \
           --jar=gs://ot-snapshots/...
```

#### Load with custom configuration

Add to your run either commandline or sbt task Intellij IDEA `-Dconfig.file=application.conf` and it
will load the configuration from your `./` path or project root. Missing fields will be resolved
with `reference.conf`.

The same happens with logback configuration. You can add `-Dlogback.configurationFile=application.xml` and
have a logback.xml hanging on your project root or run path. An example log configuration
file

```xml
<configuration>

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%level %logger{15} - %message%n%xException{10}</pattern>
        </encoder>
    </appender>

    <root level="WARN">
        <appender-ref ref="STDOUT" />
    </root>

    <logger name="io.opentargets.openfda" level="DEBUG"/>
    <logger name="org.apache.spark" level="WARN"/>

</configuration>
```
If you are using the Dataproc cluster you need to add some additional arguments specifying
where the configuration can be found. 

```sh
gcloud dataproc jobs submit spark \
           --cluster=etl-cluster \
           --project=open-targets-eu-dev \
           --region=europe-west1 \
           --async \
           --files=application.conf \
           --properties=spark.executor.extraJavaOptions=-Dconfig.file=job.conf,spark.driver.extraJavaOptions=-Dconfig.file=application.conf \
           --jar=gs://ot-snapshots/...
```
where `application.conf` is a subset of `reference.conf`

```hocon
common {
  output = "gs://ot-snapshots/etl/mk-latest"
}
```

#### Spark-submit

The fat jar can be executed on a local installation of Spark using `spark-submit`:

```shell script
/usr/lib/spark/bin/spark-submit --class io.opentargets.openfda.Main \
--driver-memory $(free -g | awk '{print $7}')g \
--master local[*] \
<jar> --arg1 ... --arg2 ...
```



### Obtaining data inputs

The following sections outline how to obtain the necessary input files for the `platformDataProcessFDA.sc ` script.

### Generate the indices dump from ES7

You will need to either connect to a machine containing the ES or forward the ssh port from it
```sh
elasticdump --input=http://localhost:9200/<indexyouneed> \
    --output=<indexyouneed>.json \
    --type=data  \
    --limit 10000 \
    --sourceOnly
```

#### Produce the raw json from scratch

In the case you need to obtain the FDA FAERS data these bash commands can be used to produce it

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

#### Blacklist

The blacklist is a manually curated txt file to exclude events that are uninformative for our purposes. This is passed as a parameter to the `platformDataProcessFDA.sc` script. 

### Outputs

Spark will write the json results in many individual files as this allows each thread to write concurrently, rather
than having to aggregate the data onto a single node before writing and is much faster. If you want all of the information
in a single file the following command can be used: 

`touch results.jsonl; find <directory with results> -name 'part*.json' -exec cat {} \; >> results.jsonl `


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
