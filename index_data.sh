#!/usr/bin/env bash

FILE=./downloads/allCountries.txt

if [ ! -f "$FILE" ]; then
    echo "$FILE does not exist. Downloading it from https://download.geonames.org/export/dump/allCountries.zip ..."
    wget "https://download.geonames.org/export/dump/allCountries.zip" -P ./downloads
    echo "Unzipping ..."
    unzip ./downloads/allCountries.zip -d ./downloads && rm ./downloads/allCountries.zip
fi

echo "Compiling JAR from source ..."
sbt clean assembly

echo "Reading config from application.properties file ..."
. application.properties

index=http://$elastic_host:$elastic_port/$elastic_index_name
echo "Elastic index: $index"

curl -XDELETE "$index"
curl -XPUT "$index" -d @settings.json -H 'Content-Type: application/json'

echo "Submitting job to Spark ..."
/usr/local/spark/bin/spark-submit target/scala-2.11/elasticsearch-geonames-locator-assembly-1.0.jar

