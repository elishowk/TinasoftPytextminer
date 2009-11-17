# -*- coding: utf-8 -*-

import codecs
import csv
import PyTextMiner

from datetime import datetime
#corpusID;docID;docAuthor;docTitle;docAbstract;index1;index2

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
   
    def ngramDocFreq(self, targetType):
        file = codecs.open(self.filepath, "w")
        #writer = csv.writer(file, dialect=self.dialect)
        file.write("ngram;frequency\n") 
        for key, value in self.corpus.ngramDocFreq(targetType).iteritems():
            #writer.writerow({'ngram' : key, 'freq' : value})
            file.write("%s;%s\n"%(key, value))
                
class Importer (PyTextMiner.Data.Importer):


    def __init__(self,
            filepath,
            corpusName='test-csv-corpus',
            titleField='docTitle',
            timestampField='date',
            datetime=None,
            contentField='docAbstract',
            authorField='docAuthor',
            corpusNumberField='corpusID',
            docNumberField='docID',
            minSize='2',
            maxSize='3',
            delimiter=';',
            quotechar='"',
            locale='en_US.UTF-8',
            **kwargs 
        ):
        self.corpusName=corpusName
        self.filepath = filepath
        
        
        
        # locale management
        self.locale = locale
        self.datetime = datetime

        self.titleField = titleField
        self.timestampField = timestampField
        self.contentField = contentField
        self.authorField = authorField
        self.corpusNumberField = corpusNumberField
        self.docNumberField = docNumberField
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
        #self.corpusNumber = tmp.next()[corpusNumberField]
        
        f2 = codecs.open(filepath,'rU')
        self.csv = csv.DictReader(
                f2,
                self.fieldNames,
                delimiter=self.delimiter,
                quotechar=self.quotechar)
        self.csv.next()
                
        
    def corpora(self, corporaID):
        corpora = PyTextMiner.Corpora(id=corporaID)

        for doc in self.csv:
            found = 0
            for corpus in corpora.corpora:
                if corpus.number == doc[self.corpusNumberField]:
                    print "found existing corpus"
                    corpus.documents += [ self.document( corpus, doc) ]
                    found = 1
                    break
            if found == 1:
                continue
            print "create new corpus if not exists :", doc[self.corpusNumberField]
            corpus = PyTextMiner.Corpus(
                    name = doc[self.corpusNumberField],
                    number = doc[self.corpusNumberField],
            )
            corpus.documents += [ self.document( corpus, doc) ]
            corpora.corpora += [corpus]

        return corpora
            
    def document(self, corpus, doc):
        # content extraction
        content =  doc[self.contentField]
        # time management
        if self.datetime is None:
            date = datetime(doc[self.timestampField])
        else:
            date = self.datetime
        document = PyTextMiner.Document(
            rawContent=doc, 
            title=doc[self.titleField],
            author=doc[self.authorField],
            number=doc[self.docNumberField],
            targets=[PyTextMiner.Target(
                rawTarget=content,  
                locale=self.locale,
                minSize=self.minSize,
                maxSize=self.maxSize,
                type=self.contentField
                )],
            date=date
        )
        return document
