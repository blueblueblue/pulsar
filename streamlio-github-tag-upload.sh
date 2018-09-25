#!/bin/bash
set -e

export GITHUB_TOKEN=$GITHUB_TOKEN
export URL="streamlio/incubator-pulsar"
export TAG=$(echo $commitTag | cut -c2-)

export ID=$(curl -s https://api.github.com/repos/$URL/releases/tags/$commitTag | jq '.id')

export FILE="./distribution/server/target/apache-pulsar-$TAG-bin.tar.gz"
printf "Uploading $FILE ... \n === \n"
curl -H "Authorization: token $GITHUB_TOKEN" -H "Content-Type: $(file -b --mime-type $FILE)" --data-binary @$FILE "https://uploads.github.com/repos/$URL/releases/$ID/assets?name=$(basename $FILE)"

export FILE="./distribution/server/target/apache-pulsar-$TAG-src.tar.gz"
printf "Uploading $FILE ... \n === \n"
curl -H "Authorization: token $GITHUB_TOKEN" -H "Content-Type: $(file -b --mime-type $FILE)" --data-binary @$FILE "https://uploads.github.com/repos/$URL/releases/$ID/assets?name=$(basename $FILE)"

export FILE="./distribution/io/target/apache-pulsar-io-connectors-$TAG-bin.tar.gz"
printf "Uploading $FILE ... \n === \n"
curl -H "Authorization: token $GITHUB_TOKEN" -H "Content-Type: $(file -b --mime-type $FILE)" --data-binary @$FILE "https://uploads.github.com/repos/$URL/releases/$ID/assets?name=$(basename $FILE)"

export FILE="./distribution/offloaders/target/apache-pulsar-offloaders-$TAG-bin.tar.gz"
printf "Uploading $FILE ... \n === \n"
curl -H "Authorization: token $GITHUB_TOKEN" -H "Content-Type: $(file -b --mime-type $FILE)" --data-binary @$FILE "https://uploads.github.com/repos/$URL/releases/$ID/assets?name=$(basename $FILE)"
