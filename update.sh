#!/bin/bash

echo remember to do cabal update

./precompute/precompute.py > bulk.jsonlines

split --line-bytes 800M bulk.jsonlines bulk- --additional-suffix .jsonlines

for n in bulk-*.jsonlines; do
  jq -s < $n > $(basename $n .jsonlines).json
done
