#!/usr/bin/env bash

sbt clean assembly

curl -XDELETE http://localhost:9200/geonames

curl -XPUT http://localhost:9200/geonames -d @settings.json -H 'Content-Type: application/json'

/usr/local/spark/bin/spark-submit target/scala-2.11/elasticsearch-geonames-locator-assembly-1.0.jar

