#!/bin/bash

machine_cores=32
job_prefix=$(uuidgen -r)
cluster_name="etl-openfda-faers"
job_name="openfda-${job_prefix}"
job_conf= # path to configuration file
job_jar= # path to jar

gcloud beta dataproc clusters create \
    $cluster_name \
    --image-version=1.5-debian10 \
    --properties=yarn:yarn.nodemanager.vmem-check-enabled=false,spark:spark.debug.maxToStringFields=1024,spark:spark.master=yarn \
    --single-node \
    --master-machine-type=n1-highmem-$machine_cores \
    --master-boot-disk-size=1000 \
    --zone=europe-west1-d \
    --project=open-targets-eu-dev \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --max-idle=10m && \
        gcloud dataproc jobs submit spark \
            --id=$job_name \
            --cluster=$cluster_name \
            --project=open-targets-eu-dev \
            --region=europe-west1 \
            --driver-log-levels io.opentargets=DEBUG \
            --files=$job_conf \
            --properties=spark.executor.extraJavaOptions=-Dconfig.file=$job_conf,spark.driver.extraJavaOptions=-Dconfig.file=$job_conf \
            --jar=$job_jar &

wait

