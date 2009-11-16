# -*- coding: utf-8 -*-

import codecs
import csv
import PyTextMiner

#corpusID;docID;docAuthor;docTitle;docAbstract;index1;index2

class Importer (PyTextMiner.Data.Importer):
    def __init__(self,
            filepath,
            corpusName='test-csv-corpus',
            
            titleField='docTitle',
            timestampField='date',
            contentField='docAbstract',
            delimiter=';',
            quotechar='"'
        ):
        self.corpusName=corpusName
        self.file=codecs.open(filepath,'rU')
        self.titleField = titleField
        self.timestampField = timestampField
        self.contentField = contentField

        self.delimiter = delimiter
        self.quotechar = quotechar

        self.fieldNames = csv.reader(
                self.file,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        ).next()
        
        self.csv = csv.DictReader(
                self.file,
                self.fieldNames,
                delimiter=self.delimiter,
                quotechar=self.quotechar)
                
        self.corpus = self._create_corpus()
        
    #TODO timestamp !
    def _create_corpus(self):
        corpus = PyTextMiner.Corpus( name=self.corpusName )
        for doc in self.csv:
            content = doc[self.contentField]
            corpus.documents += [PyTextMiner.Document(
                rawContent=doc, 
                title=doc[self.titleField],
                targets=[PyTextMiner.Target(rawTarget=content,  locale='en_US.UTF-8')]
                )]
        return corpus
