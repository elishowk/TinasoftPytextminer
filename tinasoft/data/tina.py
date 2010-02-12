# -*- coding: utf-8 -*-

from tinasoft.data import tinabsddb, basecsv
from tinasoft.pytextminer import document, corpus, ngram
import codecs
import csv
import logging
_logger = logging.getLogger('TinaAppLogger')

class Importer (basecsv.Importer):

    docDict = {}
    corpusDict = {}

    def parseFile( self, corpora ):
        """parses a row to extract corpus meta-data"""
        self.corpora = corpora
        for doc in self.csv:
            tmpfields=dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusNumber = self.decode( doc[self.fields['corpusNumberField']] )
                del tmpfields['corpusNumberField']
            except Exception, exc:
                print "document parsing exception (no corpus number avalaible) : ", exc

            document = self.parseDocument( doc, tmpfields, corpusNumber )
            if document is None:
                _logger.debug( "skipping a document" )
            else:
                yield document
            if self.corpusDict.has_key(corpusNumber) and \
                corpusNumber in self.corpora['content']:
                self.corpusDict[ corpusNumber ]['content'].append( document.id )
            #print "creating new corpus"
            corp = corpus.Corpus(
                corpusNumber,
                period_start = None,
                period_end = None
            )
            corp['content'].append( document.id )
            self.corpusDict[ corpusNumber ] = corp
            self.corpora['content'][ corpusNumber ] = 1

    def parseDocument( self, doc, tmpfields, corpusNum ):
        """parses a row to extract a document object"""
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
            _logger.debug("error parsing doc "+str(docNum)+" from corpus "+str(corpusNum))
            _logger.debug(exc)
            return None
        # parsing optional fields loop and TRY
        for field in tmpfields.itervalues():
            try:
                docArgs[ field ] = self.decode( doc[ field ] )
            except Exception, exc:
                _logger.debug("unable to parse opt field "+field+" in document " + str(docNum))
                _logger.debug(exc)
        #if 'dateField' in docArgs:
        #    datestamp = docArgs[ 'dateField' ]
        #else:
        #    datestamp = None

        doc = document.Document(
            content,
            docNum,
            title,
            #datestamp=datestamp,
            #ngramMin=self.minSize,
            #ngramMax=self.maxSize,
            **docArgs
        )
        #print document.ngramMin, document.ngramMax, document.docNum, document.rawContent
        #self.docDict[ docNum ] = doc
        return doc

    def importDocument(self,
        storage,
        tokenizer,
        tagger,
        stopwords,
        document,
        documentNum,
        corpusngrams,
        corpusNum):
        """"
        Main function processing a document, ngramizer
        applying NLP methods and inserting results into storage
        TODO : indexation !
        """
        if storage( documentNum ) is None:
            _logger.debug(tokenizer.__name__+" is working on document "+ documentNum)
            sanitizedTarget = tokenizer.sanitize(
                document['content'],
                document['forbChars'],
                document['ngramEmpty']
            )
            #document.targets.append( sanitizedTarget )
            #print target.sanitizedTarget
            sentenceTokens = tokenizer.tokenize(
                text = sanitizedTarget,
                emptyString = document['ngramEmpty'],
            )
            for sentence in sentenceTokens:
                document['tokens'] += [tagger.posTag( sentence )]

            docngrams = tokenizer.ngramize(
                minSize = document['ngramMin'],
                maxSize = document['ngramMax'],
                tokens = document['tokens'],
                emptyString = document['ngramEmpty'],
                stopwords = stopwords,
            )
            assocDocIter = []
            for ngid, ng in docngrams.iteritems():
                # save doc occs and delete
                docOccs = ng.occs
                del ng.occs
                assocDocIter += [( ng['id'], document['id'], docOccs )]
                # update corpusngrams index, replacing occs with corpus occs
                if ngid in corpusngrams:
                    corpusngrams[ ngid ].occs += 1
                else:
                    ng.occs = 1
                    corpusngrams[ ngid ] = ng
            del docngrams
            storage.insertmanyAssocNGramDocument( assocDocIter )
            # clean full text before DB storage
            document.content = ""
            document.tokens = []
            document.targets = []
            storage.insertDocument( document.id, document.date, document )

        # anyway, insert a new Doc-Corpus association
        storage.insertAssocDocument( documentNum, corpusNum )
        return corpusngrams

    """OBSOLETE"""
    def importCorpus(self,
        storage,
        corpus,
        corpusNum,
        tokenizer,
        tagger,
        stopwords,
        corpora,
        docDict):
        """OBSOLETE"""
        """yields each document in a corpus"""
        corpusngrams = {}
        # Documents processing
        for documentNum in corpus.content:
            _logger.debug( str(len( docDict )) + " documents left to import" )
            # get the document object
            document = docDict[ documentNum ]
            del docDict[ documentNum ]
            yield document
            # main NLP + ngramization method
            #corpusngrams = self.importDocument( storage, tokenizer, tagger, stopwords, document, documentNum, corpusngrams, corpusNum )
            # empty memory
            del document
        # prepares NGram-Corpus associations
        #assocNGramCorpus=[]
        #for ngid in corpusngrams.keys():
        #    assocNGramCorpus.append( ( ngid, corpusNum, corpusngrams[ ngid ]['occs'] ) )
        # stores the corpus, ngrams and corpus-ngrams associations
        #storage.insertmanyNGram( corpusngrams.items() )
        #storage.insertmanyAssocNGramCorpus( assocNGramCorpus )
        #storage.insertCorpus( corpus )
        #storage.insertAssocCorpus( corpus.id, corpora.id )

        # docDict contains the documents that remains
