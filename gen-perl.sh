#!/bin/bash

thrift \
  --gen perl \
  --allow-64bit-consts \
  -strict \
  -recurse \
  -I ./thrift-inc \
  thrift-inc/service/if/hive_service.thrift

find ./gen-perl -name '*.pm' \
| xargs perl -p -i -e 's/^package/package\n  /';

