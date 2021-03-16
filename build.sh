#!/bin/bash

set -eu

scriptdir=$(cd $(dirname $0) && pwd)

build_cmd='cd /build &&  mvn clean package'

case "$1" in
  build)
    docker build -t facets-build .
    docker run --rm=true -v /var/run/docker.sock:/var/run/docker.sock  --volume="${scriptdir}:/build"  --memory 2g --user 0 facets-build bash -c "$build_cmd"
    ;;

esac