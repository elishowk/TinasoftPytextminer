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
whitelist="&whitelist=tests/data/pubmed_whitelist.csv"
whitelistlabel="&whitelistlabel=testwhitelist"
userstopwords="&userstopwords=tests/data/user_stopwords.csv"
id="&id=testdata"


echo "POST requests"

url="http://localhost:8888/dataset"
echo $url
#POST $url
url="http://localhost:8888/corpus"
echo $url
#POST $url
url="http://localhost:8888/document"
echo $url
#POST $url
url="http://localhost:8888/ngram"
echo $url
#POST $url
url="http://localhost:8888/file"
echo $url
curl http://localhost:8888/file -d dataset="test_data_set" -d path="tests/data/pubmed_tina_test.csv"
url="http://localhost:8888/whitelist"
echo $url
curl http://localhost:8888/whitelist -d path="tests/data/pubmed_whitelist.csv" -d whitelistlabel="testwhitelist"
url="http://localhost:8888/cooccurrences"
echo $url
curl http://localhost:8888/cooccurrences -d dataset="test_data_set" -d whitelist="tests/data/pubmed_whitelist.csv" -d periods="1"
url="http://localhost:8888/graph"
echo $url
curl http://localhost:8888/graph -d dataset="test_data_set" -d periods="1"

echo "GET requests"

url="http://localhost:8888/dataset?$dataset"
echo $url
GET $url
url="http://localhost:8888/corpus?$dataset&id=1"
echo $url
GET $url
url="http://localhost:8888/document?$dataset&id=16202466"
echo $url
GET $url
url="http://localhost:8888/ngram?$dataset&id=12672038114"
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
