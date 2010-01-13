# -*- coding: utf-8 -*-

from tinasoft.data import sqlite, basecsv
from tinasoft.pytextminer import document, corpus, ngram
import codecs
import csv

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
                document.content,
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
            document.content = ""
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
        # filter NGrams, keeps the corpus-wide value in ng ['occs']
        ( corpusngrams, delList, assocNgramIter ) = ngram.NGramHelpers.filterUnique( corpusngrams, 1, corpusNum, self.encode )
        # stores the corpus, ngrams and corpus-ngrams associations
        self.storemanyNGram( corpusngrams.items() )
        self.storemanyAssocNGramCorpus( assocNgramIter )
        self.storeCorpus( corpusNum, corpus.period_start, corpus.period_end, corpus )
        self.storeAssocCorpus( corpusNum, corpora.name )
        # removes inexistant ngram-document associations
        self.cleanAssocNGramDocument( corpusNum )
        # destroys Corpus-NGram dict
        del corpusngrams
        return docDict


class Importer (basecsv.Importer):

    def corpora( self, corpora ):
        for doc in self.csv:
            tmpfields=dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusNumber = self.decode( doc[self.fields['corpusNumberField']] )
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
            if self.corpusDict.has_key(corpusNumber) and corpusNumber in corpora.content:
                self.corpusDict[ corpusNumber ].content.append( document.id )
                found = 1
            else:
                found = 0
            if found == 1:
                continue
            #print "creating new corpus"
            corp = corpus.Corpus(
                corpusNumber,
                period_start = None,
                period_end = None
            )
            corp.content.append( document.id )
            self.corpusDict[ corpusNumber ] = corp
            corpora.content.append( corpusNumber )
        return corpora

    def parseDocument( self, doc, tmpfields, corpusNum ):

        docArgs = {}
        # parsing TRY
        try:
            # get required fields
            docNum = self.decode( doc[tmpfields[ 'docNumberField' ]] )
            content = self.decode( doc[tmpfields[ 'contentField' ]] )
            title  = self.decode( doc[tmpfields[ 'titleField' ]] )
            del tmpfields['docNumberField']
            del tmpfields['contentField']
            del tmpfields['titleField']
        except Exception, exc:
            print "Error parsing doc %d from corpus %d : %s\n" % (docNum, corpusNum, exc)
            return None
        # parsing optional fields loop and TRY
        for key, field in tmpfields.iteritems():
            try:
                docArgs[ key ] = self.decode( doc[ field ] )
            except Exception, exc:
                print "warning : unable to parse optional field %s in document %s : %s" % (field, docNum, exc)
        if 'dateField' in docArgs:
            datestamp = docArgs[ 'dateField' ]
        else:
            datestamp = None

        doc = document.Document(
            content,
            datestamp,
            docNum,
            title,
            ngramMin=self.minSize,
            ngramMax=self.maxSize,
            **docArgs
        )
        #print document.ngramMin, document.ngramMax, document.docNum, document.rawContent
        self.docDict[ docNum ] = doc
        return doc
