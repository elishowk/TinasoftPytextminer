# -*- coding: utf-8 -*-

import codecs
import csv
from datetime import datetime

import PyTextMiner

class Exporter (PyTextMiner.Data.Exporter):

    def __init__(self,
                filepath,
                corpus,
                delimiter=';',
                quotechar='"',
                locale='en_US.UTF-8',
                dialect='excel'):
        self.filepath = filepath
        self.corpus = corpus
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.locale = locale
        self.dialect = dialect
   
    def ngramDocFreq(self):
        file = codecs.open(self.filepath, "w")
        #writer = csv.writer(file, dialect=self.dialect)
        file.write("ngram;frequency\n") 
        for ng in self.corpus.ngramDocFreqTable.itervalues():
            #writer.writerow({'ngram' : key, 'freq' : value})
            file.write("%s;%s\n"%(ng.strRepr, ng.occs))
                
class Importer (PyTextMiner.Data.Importer):

    def __init__(self,
            filepath,
            titleField='prop_titl',
            datetimeField='date',
            datetime=None,
            contentField='abst',
            authorField='prop_acrnm',
            corpusNumberField='Batch',
            docNumberField='prop_num',
            sumCostField='sum_cost',
            sumGrantField='sum_grant',
            minSize='1',
            maxSize='4',
            delimiter=',',
            quotechar='"',
            locale='en_US.UTF-8',
            **kwargs 
        ):
        self.filepath = filepath
        
        # locale management
        self.locale = locale
        self.datetime = datetime

        self.titleField = titleField
        # TODO
        #self.datetimeField = timestampField
        self.contentField = contentField
        self.authorField = authorField
        self.corpusNumberField = corpusNumberField
        self.docNumberField = docNumberField
        self.sumCostField = sumCostField
        self.sumGrantField = sumGrantField
        self.minSize = minSize
        self.maxSize = maxSize

        self.delimiter = delimiter
        self.quotechar = quotechar

        f1 = codecs.open(filepath,'rU')
        tmp = csv.reader(
                f1,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
        self.fieldNames = tmp.next()
        del f1
        
        f2 = codecs.open(filepath,'rU')
        self.csv = csv.DictReader(
                f2,
                self.fieldNames,
                delimiter=self.delimiter,
                quotechar=self.quotechar)
        del f2
        self.csv.next()
                
        
    def corpora( self, corpora ):
        docSet = set([])
        for doc in self.csv:
            try:
                corpusNumber = doc[self.corpusNumberField]
            except Exception, exc:
                print "document parsing exception : ", exc
                continue
                pass
            document = self.document( doc )
            found = 0
            #for corpus in corpora.corpora:
                if corpusNumber in corpora.corpora:
                    print "found existing corpus"
                    docSet.add( document.docNum )
                    found = 1
                    #break
                else:
                    found = 0
            if found == 1:
                continue
            print "create new corpus if not exists :"
            corpus = PyTextMiner.Corpus(
                name = corpusNumber,
            )
            corpus.documents.add( document.docNum )
            corpora.corpora.add( corpus.name )
        return corpora
            
    def document(self, doc):
        try:
            # TODO time management
            #if self.datetime is None:
            #    date = datetime(doc[self.datetimeField])
            #else:
            #    date = self.datetime
            content = doc[self.contentField]
            title=doc[self.titleField]
            author=doc[self.authorField]
            docNum=doc[self.docNumberField]
            #keywords=doc[self.keywordsField]
            index1=doc[self.sumCostField]
            index2=doc[self.sumGrantField]
        except Exception, exc:
            print "document parsing exception : ",exc
            pass
        print content

        target = PyTextMiner.Target(
                rawTarget=content,
                type=self.contentField,
                ngramMin=1,
                ngramMax=4,
        )

        document = PyTextMiner.Document(
            rawContent=doc,
            title=title,
            author=author,
            docNum=docNum,
            #date=date,
            #keywords=keywords,
            index1=index1,
            index2=index2
        )
        print id(target)
        document.targets.add( id(target) )
        return document
