# -*- coding: utf-8 -*-

import codecs
import csv
import time

import PyTextMiner
from PyTextMiner.Data import sqlite, basecsv

class Exporter (sqlite.Engine):

    def importDocument(self, 
        tokenizer, 
        tagger,
        stopwords,
        document,
        documentNum,
        corpusngrams,
        corpusNum):
        # only process document if unavailable in DB
        if self.loadDocument( documentNum ) is None:

            print "----- %s on document %s ----\n" % (tokenizer.__name__, documentNum)      
            sanitizedTarget = tokenizer.sanitize(
                document.rawContent,
                document.forbChars,
                document.ngramEmpty
            )
            #document.targets.append( sanitizedTarget )
            #print target.sanitizedTarget
            sentenceTokens = tokenizer.tokenize(
                text = sanitizedTarget,
                emptyString = document.ngramEmpty,
            )
            for sentence in sentenceTokens:
                document.tokens.append( tagger.posTag( sentence ) )

            docngrams = tokenizer.ngramize(
                minSize = document.ngramMin,
                maxSize = document.ngramMax,
                tokens = document.tokens,
                emptyString = document.ngramEmpty, 
                stopwords = stopwords,
            )
            #self.storemanyNGram( docngrams.values() )
            assocDocIter = []
            for ngid, ng in docngrams.iteritems():
                docOccs = ng['occs']
                del ng['occs']
                assocDocIter.append( ( ngid, int(documentNum), docOccs ) )
                # update corpusngrams index
                if ngid in corpusngrams:
                    corpusngrams[ ngid ]['occs'] += 1
                else:
                    corpusngrams[ ngid ] = ng
                    corpusngrams[ ngid ]['occs'] = 1
            del docngrams
            self.storemanyAssocNGramDocument( assocDocIter )
            # clean full text before DB storage
            document.rawContent = ""
            document.tokens = []
            #document.targets = []
            datestamp = document.datestamp
            self.storeDocument( documentNum, datestamp, document )

        # anyway, insert a new Doc-Corpus association
        self.storeAssocDocument( documentNum, corpusNum )
        return corpusngrams

    def importCorpus(self,
        corpus,
        corpusNum,
        tokenizer, 
        tagger,
        stopwords,
        corpora,
        docDict):
        corpusngrams = {}
        # Documents processing
        for documentNum in corpus.documents:
            # get the document object
            document = docDict[ documentNum ]
            corpusngrams = self.importDocument( tokenizer, tagger, stopwords, document, documentNum, corpusngrams, corpusNum )
            del document
            del docDict[ documentNum ]
            # TODO modulo 10 docs
            print ">> %d documents left to import\n" % len( docDict )
        # end of document extraction
        # clean NGrams
        ( corpusngrams, delList, assocNgramIter ) = PyTextMiner.NGramHelpers.filterUnique( corpusngrams, 2, corpusNum, self.encode )
        # stores the corpus, ngrams and corpus-ngrams associations
        self.storemanyNGram( corpusngrams.items() )
        self.storemanyAssocNGramCorpus( assocNgramIter )
        self.storeCorpus( corpusNum, corpus.period_start, corpus.period_end, corpus )
        self.storeAssocCorpus( corpusNum, corpora.name )
        # removes inexistant ngram-document associations
        self.cleanAssocNGramDocument( corpusNum )
        # process CoWord analysis
        #coword = PyTextMiner.CoWord.SimpleAnalysis()
        #matrix = coword.processCorpus( self, corpusNum )
        #self.insertmanyCooc( matrix.values() )
        # destroys Corpus-NGram dict
        del corpusngrams
        return docDict


class Importer (basecsv.Importer):

    def corpora( self, corpora ):
        for doc in self.csv:
            tmpfields=dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusNumber = self.decodeField( doc[self.fields['corpusNumberField']], 'corpusNumberField', None, None )
                del tmpfields['corpusNumberField']
            except Exception, exc:
                print "document parsing exception (no corpus number avalaible) : ", exc
                continue
                #pass

            document = self.parseDocument( doc, tmpfields, corpusNumber )
            if document is None:
                print "skipping document"
                continue
            found = 0
            if self.corpusDict.has_key(corpusNumber) and corpusNumber in corpora.corpora:
                self.corpusDict[ corpusNumber ].documents.append( document.docNum )
                found = 1
            else:
                found = 0
            if found == 1:
                continue
            #print "creating new corpus"
            corpus = PyTextMiner.Corpus(
                name = corpusNumber,
                period_start = None,
                period_end = None
            )
            corpus.documents.append( document.docNum )
            self.corpusDict[ corpusNumber ] = corpus
            corpora.corpora.append( corpusNumber )
        return corpora
            
    def parseDocument( self, doc, tmpfields, corpusNum ):
    
        docArgs = {}
        # parsing TRY
        try: 
            # get required fields
            docNum = self.decodeField( doc[tmpfields[ 'docNumberField' ]], 'docNumberField', None, corpusNum )
            content = self.decodeField( doc[tmpfields[ 'contentField' ]], 'contentField', docNum, corpusNum )
            del tmpfields['docNumberField']
            del tmpfields['contentField']
        except Exception, exc:
            print "Error parsing doc %d from corpus %d : %s\n", docNum, corpusNum, exc
            return None
        # parsing optional fields loop and TRY
        for key, field in tmpfields.iteritems():
            try:
                docArgs[ key ] = self.decodeField( doc[ field ], field, docNum, corpusNum )
            except Exception, exc:
                print "warning : unable to parse optional field %s in document %s : %s" % (field, docNum, exc)
        if 'dateField' in docArgs:
            datestamp = docArgs[ 'dateField' ]
        else:
            datestamp = None

        document = PyTextMiner.Document(
            rawContent=content,
            docNum=docNum,
            datestamp=datestamp,
            ngramMin=self.minSize,
            ngramMax=self.maxSize,
            **docArgs
        )
        #print document.ngramMin, document.ngramMax, document.docNum, document.rawContent
        self.docDict[ docNum ] = document
        return document

    def decodeField( self, field, fieldName, docNum=None, corpusNumber=None ):
        try:
            return self.decode( field )
        # TODO NOT used because of errors arg in self.decode
        except UnicodeDecodeError, uexc:
            print "Error decoding field in document from corpus\n", fieldName, docNum, corpusNumber, uexc
            return u'\ufffd'
