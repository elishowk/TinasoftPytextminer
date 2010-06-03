#!/bin/bash

echo "#############################"
echo "# TINASERVER TEST SCRIPT    #"
echo "#############################"
echo ""

dataset="&dataset=test_data_set"
periods="&period=1&period=2"
index="&index=False"
format="&format=tinacsv"
overwrite="&overwrite=False"
path="&path=tests/data/pubmed_tina_test.csv"

get_file="http://localhost:88888/file?$path$dataset$index$format$overwrite"
echo $get_file
GET $get_file
echo "end of tests"
