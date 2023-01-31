#!/bin/bash

echo remember to do cabal update

rm -rf data

mkdir data
pushd data

../precompute/precompute.py > bulk.jsonlines

split --line-bytes 400M bulk.jsonlines bulk- --additional-suffix .jsonlines

for n in bulk-*.jsonlines; do
  jq -s < $n > $(basename $n .jsonlines).json
done

popd
