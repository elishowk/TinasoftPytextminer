# -*- coding: utf-8 -*-
__author__="Elias Showk"
__all__ = ["pytextminer","data"]

# tinasoft core modules
from tinasoft.pytextminer import stopwords, indexer, tagger, tokenizer, corpora, ngram
from tinasoft.data import Engine, Reader, Writer

# checks or creates aaplication directories
from os import makedirs
from os.path import exists, join
if not exists('log'):
    makedirs('log')
if not exists('index'):
    makedirs('index')
if not exists('db'):
    makedirs('db')

# locale management
import locale
# command line utility
from optparse import OptionParser
# configuration file parsing
import yaml

# logger
import logging
import logging.handlers

class TinaApp():
    """ base class for a tinasoft.pytextminer application"""
    def __init__(
        self,
        configFile='config.yaml',
        storage=None,
        loc=None,
        stopw=None,
        index=None):
        # Set up a specific logger with our desired output level
        self.logger = logging.getLogger('TinaAppLogger')
        self.logger.setLevel(logging.DEBUG)

        # import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            return
        LOG_FILENAME = self.config['log']
        # Add the log message handler to the logger
        handler = logging.handlers.RotatingFileHandler(\
            LOG_FILENAME, maxBytes=1000000, backupCount=5)
        self.logger.addHandler(handler)
        # tries support of the locale by the host system
        try:
            if loc is None:
                self.locale = self.config['locale']
            else:
                self.locale = loc
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            self.logger.error( "locale %s was not found,\
                switching to en_US.UTF-8 by default"%self.locale)
            locale.setlocale(locale.LC_ALL, self.locale)


        options = {
            'home' : self.config['dbenv']
        }
        # connection to storage
        if storage is None:
            self.storage = Engine(self.config['storage'], **options)
        else:
            self.storage = Engine(storage)

        # connect to text-index
        if index is None:
            self.index = indexer.TinaIndex(self.config['index'])
        else:
            self.index = indexer.TinaIndex(index)
        self.logger.debug("Starting TinaApp")


    def importFile(self,
            path,
            configFile,
            corpora_id,
            overwrite=False,
            index=False,
            format= 'tina',
            filters=[]):
        """tina file import method"""
        try:
            # import import config yaml
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            self.logger.error( "Unable to read the importFile special config : " + exc)
            return
        # load Stopwords object
        self.stopwords = stopwords.StopWords( "file://%s" % self.config['stopwords'] )
        # load filters (TODO put it into import.yaml !!)
        self.filtertag = ngram.PosTagFilter()
        self.filterContent = ngram.Filter()
        self.filterstop = stopwords.StopWordFilter(
            'file:///home/elishowk/TINA/Datas/100126-fetopen-stopwords-from-david.csv'
        )
        filters=[self.filtertag,self.filterContent,self.filterstop]
        # loads the source file
        print format, path
        dsn = format+"://"+path
        #self.logger.debug(dsn)
        fileReader = Reader(dsn,
            delimiter = self.config['delimiter'],
            quotechar = self.config['quotechar'],
            locale = self.config['locale'],
            fields = self.config['fields']
        )
        self._walkFile(fileReader, corpora_id, overwrite, index, filters)

    def _walkFile(self, fileReader, corpora_id, overwrite, index, filters):
        """gets importFile() results to insert contents into storage"""
        if index is True:
            writer = self.index.getWriter()
        self.corpusngrams=[]
        corps = self.storage.loadCorpora(corpora_id)
        if corps is None:
            corps = corpora.Corpora( corpora_id )
        #self.logger.debug(corps)
        fileGenerator = fileReader.parseFile( corps )
        docEdges={}
        # fisrt part of file parsing
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                # insert doc in storage
                self.storage.insertDocument( document )
                if index is True:
                    res = self.index.write(document, writer, overwrite)
                    if res is not None:
                        self.logger.debug("document will not be overwritten")
                docngrams = self._extractNGrams( document, corpusNum,\
                    self.config['ngramMin'], self.config['ngramMax'], filters)
                # TODO export docngrams (filtered)
                #Document-corpus Association are included in the object
                if document['id'] in docEdges:
                    docEdges[document['id']]+=1
                else:
                    docEdges[document['id']]=1

        # Second part of file parsing
        except StopIteration, stop:
            if index is True:
                # commit changes to indexer
                writer.commit()
            # insert or updates corpora
            corps = fileReader.corpora
            self.storage.insertCorpora( corps )
            for corpusNum in corps['content']:
                # get the Corpus object and import
                corpus = fileReader.corpusDict[ corpusNum ]
                for doc, occ in docEdges.iteritems():
                    corpus.addEdge( 'Document', doc, occ )
                for i in range( len(self.corpusngrams) ):
                    ng=self.corpusngrams.pop()
                    corpus.addEdge( 'NGram', ng['id'], 1 )
                self.storage.insertCorpus( corpus )
                self.storage.insertAssocCorpus( corpus['id'], corps['id'] )
                del corpus
                del fileReader.corpusDict[ corpusNum ]
            return


    def _extractNGrams(self, document, corpusNum, ngramMin, ngramMax, filters):
        """"Main NLP for a document"""
        self.logger.debug(tokenizer.TreeBankWordTokenizer.__name__+\
            " is working on document "+ document['id'])
        docngrams = tokenizer.TreeBankWordTokenizer.extract( document,\
            self.stopwords, ngramMin, ngramMax, filters )
        #self.logger.debug(docngrams)
        for ngid, ng in docngrams.iteritems():
            # save document occurrencess and delete it
            docOccs = ng['occs']
            del ng['occs']
            loadNG = self.storage.loadNGram( ng['id'] )
            if loadNG is None:
                # insert a new NGram and updates the corpusngrams index
                restoreNG.addEdge( 'Document', document['id'], docOccs )
                restoreNG.addEdge( 'Corpus', corpusNum, 1 )
                self.storage.insertNGram( ng )
                self.corpusngrams+=[ng]
            else:
                # updates NGram edges
                restoreNG=ngram.NGram(loadNG)
                #self.logger.debug(restoreNG)
                restoreNG.addEdge( 'Document', document['id'], docOccs )
                restoreNG.addEdge( 'Corpus', corpusNum, 1 )
                self.storage.insertNGram( loadNG )
                self.corpusngrams+=[restoreNG]
            #self.storage.insertAssocNGramCorpus( ng['id'], corpusNum, self.corpusngrams[ ngid ]['occs'] )
        # clean tokens before ending
        #document['content'] = ""
        document['tokens'] = []
        return docngrams


    def getAllCorpus(self, raw=True):
        """fix decode/encode non optimal process"""
        corpuslst = []
        gen = self.storage.select('Corpus')
        try:
            record = gen.next()
            while record:
                corpuslst += [record[1]]
                record = gen.next()
        except StopIteration, si:
            if raw is True:
                return corpuslst
            else:
                return self.storage.encode( corpuslst )

    def exportCorpusNGram(self, corpus, filepath, **kwargs):
        """export a file containing a corpus' NGrams"""
        def printpostag(record):
            """prepares the postag field printing"""
            postag = ""
            if record[1]['postag'] is not None:
                for word in record[1]['postag']:
                    postag += "_".join([word[1],word[0]]) + ","
            return postag

        gen = self.storage.select('NGram')
        csv = Writer('basecsv://'+filepath, **kwargs)
        try:
            record = gen.next()
            while record:
                if corpus['id'] in record[1]['edges']['Corpus']:
                    #postag = printpostag(record)
                    #occs = corpus['edges']['Ngram'][record[1]['id']]
                    csv.writeRow( [record[1]['id'], record[1]['label'] ])
                record = gen.next()
        except StopIteration, si: return

    def exportAllNGrams(self, filepath, **kwargs):
        gen = self.storage.select('NGram')
        csv = Writer('basecsv://'+filepath, **kwargs)
        try:
            record = gen.next()
            while record:
                #occs = corpus['edges']['Ngram'][record[1]['id']]
                csv.writeRow( [record[1]['id'], record[1]['label'] ])
                record = gen.next()
        except StopIteration, si: return


    def exportCorpusCooc(self, corpus, filepath, **kwargs):
        generator = self.storage.selectCorpusCooc(corpus['id'])
        csv = Writer('basecsv://'+filepath, **kwargs)
        nodes = {}
        try:
            while 1:
                key,row = generator.next()
                id,month = key
                if id not in nodes:
                    nodes[id] = {
                        'edges' : {}
                    }
                for ngram, cooc in row.iteritems():
                    if ngram in nodes[id]['edges']:
                        nodes[id]['edges'][ngram] += cooc
                    else:
                        nodes[id]['edges'][ngram] = cooc
        except StopIteration, si:
            for ng1 in nodes.iterkeys():
                for ng2, cooc in nodes[ng1]['edges'].iteritems():
                    csv.writeRow([ ng1, ng2, str(cooc), corpus['id'] ])

    def createCorpus(self):
        raise NotImplemented

    def createCorpora(self):
        raise NotImplemented

    def createDocument(self):
        raise NotImplemented

    def createNGram(self):
        raise NotImplemented

    def writeCooc(self):
        raise NotImplemented
