# -*- coding: utf-8 -*-

import codecs
import csv
import PyTextMiner

from datetime import datetime
#corpusID;docID;docAuthor;docTitle;docAbstract;index1;index2

class Exporter (PyTextMiner.Data.Exporter):

    def __init__(self,
                corpus,
                delimiter=';',
                quotechar='"',
                locale='en_US.UTF-8'):
        pass
                
    def ngram_frequency(self, filepath):
        file = codecs.open(filepath, "rU")
        writer = csv.writer(file, dialect='excel')
        rows = []
        for key, value in ngramDocFreq.iteritems():
            rows.append([key, value])
        writer.writerows(rows) 
                    
                
                
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
        self.file=codecs.open(filepath,'rU')
        
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

        tmp = csv.reader(
                self.file,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
        self.fieldNames = tmp.next()
        self.corpusNumber = tmp.next()[corpusNumberField]
        
        self.csv = csv.DictReader(
                self.file,
                self.fieldNames,
                delimiter=self.delimiter,
                quotechar=self.quotechar)
                
        self.corpus = self._create_corpus()
        


            
    def _create_corpus(self):
    
        corpus = PyTextMiner.Corpus( name=self.corpusName, number=self.corpusNumber )
        for doc in self.csv:
            content = doc[self.contentField]
            
          # time management
            if self.datetime is None:
                date = datetime(doc[self.timestampField])
            else:
                date = self.datetime

            corpus.documents += [PyTextMiner.Document(
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
                )]
        return corpus
