#!/usr/bin/python
# -*- coding: utf-8 -*-

from CSVTextMiner import CSVTextMiner
#open the file
csvfile = open("t/data-proposal.csv")
#create CSVTextMiner object application
csvApp = CSVTextMiner(corpusName="test-csv-corpus", file=csvfile, titleField='proposal_title', timestampField='date', contentField='abstract')
#run the parsing of the corpus
corpus = csvApp.createCorpus()
print corpus

print "end of CSVTextMiner usage tests";


#        for d in csv.documents:
#            print "\n",d
#            print d.rawContent
#            print d.date
#

