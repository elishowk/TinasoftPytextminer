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
whitelist="&whitelist=tests/data/pubmed_whitelist.csv"
whitelistlabel="&whitelistlabel=testwhitelist"
userstopwords="&userstopwords=tests/data/user_stopwords.csv"
id="&id=testdata"

url="http://localhost:88888/dataset?$dataset"
echo $url
GET $url
url="http://localhost:88888/corpus?$id$dataset"
echo $url
GET $url
url="http://localhost:88888/document?$id$dataset"
echo $url
GET $url
url="http://localhost:88888/ngram?$id$dataset"
echo $url
GET $url
url="http://localhost:88888/file?$path$dataset$index$format$overwrite"
echo $url
GET $url
url="http://localhost:88888/whitelist?$periods$dataset$whitelistlabel"
echo $url
#GET $url
url="http://localhost:88888/cooccurrences?$path$dataset$index$format$overwrite"
echo $url
#GET $url
url="http://localhost:88888/graph?$path$periods$whitelist"
echo $url
#GET $url

echo "end of tests"
