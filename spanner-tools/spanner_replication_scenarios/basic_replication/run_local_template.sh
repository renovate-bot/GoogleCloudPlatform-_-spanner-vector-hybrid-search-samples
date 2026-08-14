#!/bin/bash
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

TEMPLATE_NAME=$1
shift
ARGS=$@

# Clone the official repo if we haven't already
if [ ! -d "DataflowTemplates" ]; then
    echo "Cloning official Google DataflowTemplates repository..."
    git clone https://github.com/GoogleCloudPlatform/DataflowTemplates.git
fi

if [ "$TEMPLATE_NAME" == "Cloud_Spanner_to_GCS_Avro" ]; then
    cd DataflowTemplates/v1
    MAIN_CLASS="com.google.cloud.teleport.spanner.ExportPipeline"
elif [ "$TEMPLATE_NAME" == "GCS_Avro_to_Cloud_Spanner" ]; then
    cd DataflowTemplates/v1
    MAIN_CLASS="com.google.cloud.teleport.spanner.ImportPipeline"
elif [ "$TEMPLATE_NAME" == "spanner-to-sourcedb" ]; then
    cd DataflowTemplates
    PROJECT_FLAG="-pl v2/spanner-to-sourcedb"
    MAIN_CLASS="com.google.cloud.teleport.v2.templates.SpannerToSourceDb"
else
    echo "Unknown template: $TEMPLATE_NAME"
    exit 1
fi

echo "Compiling and running $TEMPLATE_NAME locally via DirectRunner..."
# First, compile the module AND all its dependencies (-am), and install them to ~/.m2
mvn clean install $PROJECT_FLAG -am -DskipTests -Djib.skip=true -q

# Then, execute the Java template strictly in the target module
mvn exec:java $PROJECT_FLAG \
  -Dexec.classpathScope=test \
  -Djib.skip=true \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="--runner=DirectRunner ${ARGS}" 2>&1 | grep --line-buffered -v '\[WARNING\]'
