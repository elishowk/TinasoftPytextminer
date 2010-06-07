#!/bin/bash

echo "#############################"
echo "# TINASERVER TEST SCRIPT    #"
echo "#############################"
echo ""

dataset="&dataset=test_data_set"
periods="&periods=1&periods=2"
index="&index=False"
format="&format=tinacsv"
overwrite="&overwrite=False"
path="&path=tests/data/pubmed_tina_test.csv"
whitelist="&whitelist=tests/data/pubmed_export.csv"
whitelistlabel="&whitelistlabel=testwhitelist"
userstopwords="&userstopwords=tests/data/user_stopwords.csv"
id="&id=testdata"

url="http://localhost:8888/dataset?$dataset"
echo $url
GET $url
url="http://localhost:8888/corpus?$id$dataset"
echo $url
GET $url
url="http://localhost:8888/document?$id$dataset"
echo $url
GET $url
url="http://localhost:8888/ngram?$id$dataset"
echo $url
GET $url
url="http://localhost:8888/file?$path$dataset$index$format$overwrite"
echo $url
GET $url
url="http://localhost:8888/whitelist?$periods$dataset$whitelistlabel"
echo $url
GET $url
url="http://localhost:8888/cooccurrences?$periods$whitelist"
echo $url
GET $url
url="http://localhost:8888/graph?$dataset&filetype=gexf"
echo $url
GET $url

echo "end of tests"
