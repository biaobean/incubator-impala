#!/usr/bin/env bash
# Copyright 2014 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts up the StateStored with the specified command line arguments.

set -e

BUILD_TYPE=debug
STATESTORED_ARGS=""
BINARY_BASE_DIR=${IMPALA_HOME}/be/build

# Everything except for -build_type should be passed as a statestored argument
for ARG in $*
do
  case "$ARG" in
    -build_type=debug)
      BUILD_TYPE=debug
      ;;
    -build_type=release)
      BUILD_TYPE=release
      ;;
    -build_type=*)
      echo "Invalid build type. Valid values are: debug, release"
      exit 1
      ;;
    *)
      STATESTORED_ARGS="${STATESTORED_ARGS} ${ARG}"
      ;;
  esac
done

# If Kerberized, source appropriate vars and set startup options
if ${CLUSTER_DIR}/admin is_kerberized; then
  . ${MINIKDC_ENV}
  STATESTORED_ARGS="${STATESTORED_ARGS} -principal=${MINIKDC_PRINC_IMPA}"
  STATESTORED_ARGS="${STATESTORED_ARGS} -keytab_file=${KRB5_KTNAME}"
fi

exec ${BINARY_BASE_DIR}/${BUILD_TYPE}/statestore/statestored ${STATESTORED_ARGS}