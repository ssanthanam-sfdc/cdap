#!/usr/bin/env bash

# Copyright © 2014-2016 Cask Data, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
  
# Build script for docs

source ../vars
source ../_common/common-build.sh

CHECK_INCLUDES=${TRUE}

function download_readme_file_and_test() {
  # Downloads a README.rst file to a target directory, and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.

  local file_name='README.rst'
  
  local includes_dir=${1}
  local source_url=${2}
  local md5_hash=${3}
  local relative_path=${4}

  # Replace any path components with dashes
  local target_file_name=${relative_path//\//-}.rst

  echo "Downloading using curl ${file_name} from ${source_url}"
  curl ${source_url}/${relative_path}/${file_name} --output ${includes_dir}/${target_file_name} --silent
  test_an_include ${md5_hash} ${includes_dir}/${target_file_name}
}

function download_includes() {
  echo "Downloading source files to be included from GitHub..."
  local github_url="https://raw.githubusercontent.com/caskdata"
  local includes_dir=${1}
  set_version

  local clients_branch ingest_branch
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    clients_branch="develop"
    ingest_branch="develop"
  else
    clients_branch="${GIT_BRANCH_CDAP_CLIENTS}"
    ingest_branch="${GIT_BRANCH_CDAP_INGEST}"
  fi

# cdap-clients
# https://raw.githubusercontent.com/caskdata/cdap-clients/develop/cdap-authentication-clients/java/README.rst
  local clients_url="${github_url}/cdap-clients/${clients_branch}"

  download_readme_file_and_test ${includes_dir} ${clients_url} 9bdc7d9ab874bfb6ec044964d3df804e cdap-authentication-clients/java
  download_readme_file_and_test ${includes_dir} ${clients_url} 6f937cbf71ed2312a4893cba27e6145f cdap-authentication-clients/python
  
# cdap-ingest
# https://raw.githubusercontent.com/caskdata/cdap-ingest/develop/cdap-file-drop-zone/README.rst
  local ingest_url="${github_url}/cdap-ingest/${ingest_branch}"

  download_readme_file_and_test ${includes_dir} ${ingest_url} cf2d8cac45b4be267adbb0e8ecdc88a4 cdap-flume
  download_readme_file_and_test ${includes_dir} ${ingest_url} a852e493aff54ffd726368691f248d80 cdap-stream-clients/java
  download_readme_file_and_test ${includes_dir} ${ingest_url} da242d9be7051417bd5ff73b3dc5edc2 cdap-stream-clients/python
  download_readme_file_and_test ${includes_dir} ${ingest_url} b798091f24f6ecfe05d614f1dd1f7a03 cdap-stream-clients/ruby

  echo_red_bold "Check included example files for changes"
  test_an_include 9e137848822e63101b699af03af7f45e ../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java

  test_an_include bb7344dd2c55c5ef70653ff0b5fcd6df ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryBuilder.java
  test_an_include 80216a08a2b3d480e4a081722408222f ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryService.java
  test_an_include 29fe1471372678115e643b0ad431b28d ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java

  test_an_include f9acd1e2ed73ba5e83b9a0cd6c75f988 ../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java

  test_an_include 95092c8859fd97b6b327d774255e5dac ../../cdap-examples/WikipediaPipeline/src/main/java/co/cask/cdap/examples/wikipedia/TopNMapReduce.java

  test_an_include 8c24858e8d168c0909fd554e96a2aba9 ../../cdap-examples/WikipediaPipeline/src/main/scala/co/cask/cdap/examples/wikipedia/ClusteringUtils.scala
}

function test_includes () {
  echo "All includes tested."
}

run_command ${1}
