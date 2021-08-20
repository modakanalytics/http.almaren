#!/bin/sh

spark-submit \
  --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:1.2.0-2.4,org.postgresql:postgresql:42.2.23" \
  --class com.modakanalytics.HttpExample \
  --master local \
  ./target/scala-2.11/http-to-postgres-example_2.11-0.1.jar