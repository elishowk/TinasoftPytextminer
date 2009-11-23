#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import locale

# initialize the system path with local dependencies and pre-built libraries
import bootstrap

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Reader, Writer, sqlite

class TestFetExtract(unittest.TestCase):
    def setUp(self):
        # try to determine the locale of the current system
        try:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'fr_FR.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        self.stopwords = PyTextMiner.StopWords("file://src/t/stopwords/en.txt" )

    def test_proposal(self):
        # init of the file reader & parser
        tina = Reader("tina://src/t/pubmed_AIDS_10_format_original_badchars.fet",
            titleField='doc_titl',
            datetimeField='doc_date',
            contentField='doc_abst',
            authorField='doc_acrnm',
            corpusNumberField='corp_num',
            docNumberField='doc_num',
            index_1='index_1',
            index_2='index_2',
            minSize='1',
            maxSize='4',
            delimiter=',',
            quotechar='"',
            locale='en_US.UTF-8',
        )
        corpora = PyTextMiner.Corpora( name="pubmedTest" )
        corpora = tina.corpora( corpora )
        #print tina.corpusDict
        #sql = Writer("sqlite://src/t/output/"+corpora.name+".db", locale=self.locale)
        sql = Writer("sqlite://:memory:", locale=self.locale)
        # clean the DB contents
        sql.clear()
        # stores Corpora
        if not sql.fetch_one( PyTextMiner.Corpora, corpora.name ) :
            sql.storeCorpora( corpora.name, corpora )
        
        for corpusNum in corpora.corpora:
            # get the Corpus object
            corpus = tina.corpusDict[ corpusNum ]
            # stores it
            sql.storeCorpus( corpusNum, corpus )
            sql.storeAssocCorpus( corpusNum, corpora.name )
            # Documents processing
            for documentNum in corpus.documents:
                # only process document if unavailable in DB
                if not sql.fetch_one( PyTextMiner.Document, documentNum ):
                    # get the document object
                    document = tina.docDict[ documentNum ]
                    print "----- TreebankWordTokenizer on document %s ----\n" % documentNum
                    sanitizedTarget = PyTextMiner.Tokenizer.TreeBankWordTokenizer.sanitize(
                            document.rawContent,
                            document.forbChars,
                            document.ngramEmpty
                        )
                    document.targets.add( sanitizedTarget )
                    #print target.sanitizedTarget
                    sentenceTokens = PyTextMiner.Tokenizer.TreeBankWordTokenizer.tokenize(
                        text = sanitizedTarget,
                        emptyString = document.ngramEmpty,
                    )
                    for sentence in sentenceTokens:
                        document.tokens.append( PyTextMiner.Tagger.TreeBankPosTagger.posTag( sentence ) )
                    
                    document.ngrams = PyTextMiner.Tokenizer.TreeBankWordTokenizer.ngramize(
                        minSize = document.ngramMin,
                        maxSize = document.ngramMax,
                        tokens = document.tokens,
                        emptyString = document.ngramEmpty, 
                        stopwords = self.stopwords,
                    )
                    for ngid, ng in document.ngrams.iteritems():
                        occs = ng['occs']
                        del ng['occs']
                        sql.storeNGram( ngid, ng )
                        sql.storeAssocNGram( ngid, documentNum, occs )
                    # get the list of keys
                    document.ngrams = document.ngrams.keys()
                    # DB Storage
                    document.rawContent = ""
                    document.tokens = []
                    document.targets = set()
                    
                    sql.storeDocument( documentNum, document )
                    del document
                    del tina.docDict[ documentNum ]
                # else insert a new Doc-Corpus association
                sql.storeAssocDocument( documentNum, corpusNum )
                # TODO modulo 10 docs
                print ">> %d documents left to analyse\n" % len( tina.docDict )
            # EXPORT filter ngrams by corpus occs
            req='select count(asn.id1), ng.blob from AssocNGram as asn, NGram as ng JOIN AssocDocument as asd ON asd.id1 = asn.id2 AND asn.id1 = ng.id WHERE ( asd.id2 = ? ) GROUP BY asn.id1 HAVING count (asn.id1) > 1'
            result = sql.execute(req, [sql.encode(corpusNum)])
            dump = Writer ("tina://src/t/output/"+corpusNum+"-ngramOccPerCorpus.csv", locale=self.locale)
            # prepares csv export
            def prepareRows( row ):
                ng = sql.decode( row[1] )
                tag = []
                for toklist in ng['original']:
                    tag.append( dump.encode( "_".join( toklist ) ) )
                tag = " ".join( tag )
                return [ ng['str'], row[0], tag ]

            filtered = map( prepareRows, result )
            print "writes a corpus-wide ngram synthesis"
            dump.csvFile( ['ngram','documents','POS tagged'], filtered )

            # destroys Corpus object
            del corpus
            del tina.corpusDict[ corpusNum ]
        print "Dumping SQL db"
        sql.dump( "src/t/output/dump_"+corpora.name+".sql" )
        
        print "export a sample from document db"
        reader = PyTextMiner.Importer()
        ngramFetch = 'SELECT ng.blob FROM NGram as ng JOIN AssocNGram as asn ON asn.id1 = ng.id WHERE ( asn.id2 = ? )'
        select = 'SELECT doc.id, doc.blob from Document as doc LIMIT 10'
        result = sql.execute(select)
        print result
        docList = []
        for document in result:
            docid = document[0]
            doc = sql.decode( document[1] )
            ngFetch = sql.execute(ngramFetch, [docid])
            ngList = []
            for ng in ngFetch:
                ngObj = sql.decode( ng[0] )
                #print type(dump.encode(ngObj['str']))
                ngList += [ ngObj['str'] ]
                #ngList += [ dump.encode(ngObj['str']) ]
            doc.ngrams = ", ".join(ngList)
            #print doc.ngrams
            import jsonpickle
            docList += [jsonpickle.encode(doc)]
        #dump.objectToCsv( docList, [ 'docNum', 'rawContent', 'targets', 'title', 'author', 'index1', 'index2', 'metas', 'date', 'tokens', 'ngrams' ] )
        import codecs
        file = codecs.open("src/t/output/documentSample.csv", "w", 'utf-8', 'xmlcharrefreplace' )
        for doc in docList:
            file.write( doc + "\n" )
if __name__ == '__main__':
    unittest.main()
