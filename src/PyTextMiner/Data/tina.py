# -*- coding: utf-8 -*-

import codecs
import csv
from datetime import datetime

import PyTextMiner

class Exporter (PyTextMiner.Data.Exporter):

    def __init__(self,
                filepath,
                #corpus,
                delimiter=',',
                quotechar='"',
                locale='en_US.UTF-8',
                dialect='excel'):
        self.filepath = filepath
        #self.corpus = corpus
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.locale = locale
        self.dialect = dialect
   
    def ngramDocFreq(self, ngramDocFreqDict ):
        enc= self.locale.split('.')[1].lower()
        print enc
        file = codecs.open(self.filepath, "w", encoding=enc )
        #writer = csv.writer(file, dialect=self.dialect)
        file.write("ngram;frequency\n") 
        for ng in ngramDocFreqDict.itervalues():
            if ng['occs'] > 1:
                file.write("%s;%s\n"%(codecs.encode( ng['str'], enc, 'replace' ), ng['occs']))
                
class Importer (PyTextMiner.Data.Importer):

    def __init__(self,
            filepath,
            titleField='doc_titl',
            datetimeField='doc_date',
            datetime=None,
            contentField='doc_abst',
            authorField='doc_acrnm',
            corpusNumberField='corp_num',
            docNumberField='doc_num',
            index1Field='index_1',
            index2Field='index_2',
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
        self.encoding = locale.split('.')[1].lower()
        self.datetime = datetime

        self.titleField = titleField
        # TODO
        #self.datetimeField = timestampField
        self.contentField = contentField
        self.authorField = authorField
        self.corpusNumberField = corpusNumberField
        self.docNumberField = docNumberField
        self.index1Field = index1Field
        self.index2Field = index2Field
        self.minSize = minSize
        self.maxSize = maxSize

        self.delimiter = delimiter
        self.quotechar = quotechar

        f1 = self.open( filepath )
        tmp = csv.reader(
                f1,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
        self.fieldNames = tmp.next()
        del f1
        
        f2 = self.open( filepath )
        self.csv = csv.DictReader(
                f2,
                self.fieldNames,
                delimiter=self.delimiter,
                quotechar=self.quotechar)
        self.csv.next()
        del f2
        self.docDict = {}
        self.corpusDict = {}

    def open( self, filepath ):
        return codecs.open(filepath,'rU', errors='replace' )
    
    def unicode( self, text ):
        return unicode( text, errors='replace' )

    def corpora( self, corpora ):
        for doc in self.csv:
            try:
                corpusNumber = self.unicode(doc[self.corpusNumberField])
                #print "CORPUS NUMBER ", corpusNumber
            except Exception, exc:
                print "document parsing exception : ", exc
                continue
                pass
            document = self.document( doc )
            if document is None:
                print "skipping document"
                continue
            found = 0
            if self.corpusDict.has_key(corpusNumber) and corpusNumber in corpora.corpora:
                #print "found existing corpus"

                self.corpusDict[ corpusNumber ].documents.add( document.docNum )
                found = 1
            else:
                found = 0
            if found == 1:
                continue
            #print "creating new corpus"
            corpus = PyTextMiner.Corpus(
                name = corpusNumber,
            )
            corpus.documents.add( document.docNum )
            self.corpusDict[ corpusNumber ] = corpus
            corpora.corpora.add( corpusNumber )
        return corpora
            
    def document( self, doc ):
    
        try:
            docNum = self.unicode(doc[self.docNumberField])
            if self.docDict.has_key( docNum ):
                return self.docDict[ docNum ]
            # TODO time management
            #if self.datetime is None:
            #    date = datetime(doc[self.datetimeField])
            #else:
            #    date = self.datetime
            content = self.unicode(doc[self.contentField])
            title = self.unicode(doc[self.titleField])
            author = self.unicode(doc[self.authorField])
            #keywords=doc[self.keywordsField]
            index1 = self.unicode(doc[self.index1Field])
            index2 = self.unicode(doc[self.index2Field])
        except Exception, exc:
            print "document parsing exception : ",exc
            return None
            

        document = PyTextMiner.Document(
            rawContent=content,
            title=title,
            author=author,
            docNum=docNum,
            #date=date,
            #keywords=keywords,
            index1=index1,
            index2=index2,
            ngramMin=1,
            ngramMax=4,
        )
        #print "TARGET =========", id(target)
        #document.targets.add( id(target) )
        self.docDict[ docNum ] = document
        return document
