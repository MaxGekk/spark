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
# build.sh <out-dir> [<jdk-source-tree>]
# Builds the WhiteBox jar from the JDK source tree's test library and the spike's jar (the agent,
# the counts helper, the probe and the replay wrapper) into <out-dir>.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out=$(realpath -m "$1"); jdk=${2:-/home/max/proj/openjdk-build/jdk25}
mkdir -p "$out/wb" "$out/cls"
javac -d "$out/wb" $(find "$jdk/test/lib/jdk/test/whitebox" -name '*.java')
jar cf "$out/wb.jar" -C "$out/wb" .
javac --add-modules jdk.incubator.vector -cp "$out/wb.jar" -d "$out/cls" "$here"/src/*.java
jar cfm "$out/probe.jar" "$here/src/MANIFEST.txt" -C "$out/cls" .
echo "built $out/wb.jar and $out/probe.jar"
