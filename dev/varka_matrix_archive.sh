#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Archives a build of the test classes for the fanned-out option matrix (VARKA-507), or unpacks
# one into a build directory:
#
#   dev/varka_matrix_archive.sh pack BUILD_DIR TAR      # BUILD_DIR as dev/varka_matrix.sh left it
#   dev/varka_matrix_archive.sh unpack TAR BUILD_DIR
#
# The archive holds every entry of the test classpath - the build's class directories and the
# dependencies' jars alike - at its absolute path, which is the same on every GitHub runner, and
# the build directory's classpath and jvm.opts under the names `varka-classpath` and
# `varka-jvm.opts` at the archive's root. Unpacking restores the entries where they were and
# writes those two files into the build directory a bin reads with --skip-build --build-dir, so
# the pull request's Build, which builds into target/varka-scoped, and the matrix's own build,
# into target/varka-matrix, give the bins the same thing.
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-}" in
  pack)
    build=${2:?usage: pack BUILD_DIR TAR} tar=${3:?usage: pack BUILD_DIR TAR}
    [ -s "$build/classpath" ] && [ -s "$build/jvm.opts" ] || {
      echo "no classpath or jvm.opts in $build" >&2; exit 1; }
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    cp "$build/classpath" "$work/varka-classpath"
    cp "$build/jvm.opts" "$work/varka-jvm.opts"
    tr ':' '\n' < "$build/classpath" | grep -v '^$' \
      | while read -r e; do if [ -e "$e" ]; then echo "$e"; fi; done > "$work/entries"
    mkdir -p "$(dirname "$tar")"
    tar -cPf "$tar" -T "$work/entries" -C "$work" varka-classpath varka-jvm.opts
    ls -l "$tar"
    ;;
  unpack)
    tar=${2:?usage: unpack TAR BUILD_DIR} build=${3:?usage: unpack TAR BUILD_DIR}
    mkdir -p "$build"
    tar -xPf "$tar" -C "$build" --transform 's#^varka-classpath$#classpath#' \
      --transform 's#^varka-jvm\.opts$#jvm.opts#'
    [ -s "$build/classpath" ] && [ -s "$build/jvm.opts" ] || {
      echo "$tar holds no classpath or jvm.opts" >&2; exit 1; }
    ;;
  *)
    echo "usage: dev/varka_matrix_archive.sh pack BUILD_DIR TAR | unpack TAR BUILD_DIR" >&2
    exit 2
    ;;
esac
