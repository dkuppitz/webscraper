#!/bin/bash

mvn -q exec:java -Dexec.mainClass="guru.gremlin.webscraper.App" -Dexec.args=$@

if [ $? -eq 2 ]; then
  echo
  echo "Usage:"
  echo "       $0 <output> [<url>] [<max-depth>]"
  echo
  exit 1
fi
