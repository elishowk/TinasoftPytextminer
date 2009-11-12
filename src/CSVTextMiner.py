# -*- coding: utf-8 -*-
import PyTextMiner
import csv

class CSVTextMiner:
    def __init__(self,
            corpusName,
            file,
            titleField,
            timestampField,
            contentField,
            corporaID,
            corpora=None,
            delimiter=';',
            quotechar='"',
        ):
        self.corpusName=corpusName
        self.file=file
        self.titleField = titleField
        self.timestampField = timestampField
        self.contentField = contentField
        if corpora is None:
            self.corpora=PyTextMiner.Corpora(id=corporaID)
        else:
            self.corpora=corpora
        self.delimiter = delimiter
        self.quotechar = quotechar
        MyCsv = csv.reader(
                file,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
        # first line contains field names
        self.fieldNames = MyCsv.next()
        self.csv = csv.DictReader(
                file,
                self.fieldNames,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
    #TODO timestamp !
    def createCorpus(self):
        corpus = PyTextMiner.Corpus( name=self.corpusName )
        for doc in self.csv:
            content = doc[self.contentField]
            #print content
            #print doc[self.titleField]
            corpus.documents += [PyTextMiner.Document(
                rawContent=doc, 
                title=doc[self.titleField],
                targets=[PyTextMiner.Target(rawTarget=content)]
                )]
        #self.corpora.corpora.append(corpus)
        for document in corpus.documents:
            for target in document.targets:
    #                    print "----- RegexpTokenizer ----\n"
    #                    target.sanitizedTarget = PyTextMiner.Tokenizer.RegexpTokenizer.sanitize( input=target.rawTarget, separator=target.separator, forbiddenChars=target.forbiddenChars );
    #                    print target.sanitizedTarget
    #                    print PyTextMiner.Tokenizer.RegexpTokenizer.tokenize( text=target.sanitizedTarget, separator=target.separator ) 
    #                    print "----- NltkTokenizer ----\n"
                target.sanitizedTarget = PyTextMiner.Tokenizer.WordPunctTokenizer.sanitize( input=target.rawTarget, emptyString=target.emptyString, forbiddenChars=target.forbiddenChars );
                print target.sanitizedTarget
                target.tokens = PyTextMiner.Tokenizer.WordPunctTokenizer.tokenize( text=target.sanitizedTarget )
                print target.tokens
        return corpus
