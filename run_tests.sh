#!/bin/bash

top_path=/home/radon

code_path=${top_path}/src
venv_path=${top_path}/ve/radon-web

tasks_proj_dir=${code_path}/$1
start_tests_dir=${code_path}/$2

results_dir=$3

export LC_ALL=en_US.utf-8
export LANG=en_US.utf-8

source ${venv_path}/bin/activate

#pip install -e ${tasks_proj_dir}

cd ${start_tests_dir}
pytest --junit-xml=${results_dir}/results.xml
