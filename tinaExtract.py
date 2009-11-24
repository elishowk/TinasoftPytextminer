#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import os, sys
import locale
from optparse import OptionParser

# initialize the system path with local dependencies and pre-built libraries
import bootstrap

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Reader, Writer, sqlite

class Program:
    def __init__(self):
    
        parser = OptionParser()
        
        parser.add_option("-i", "--input", dest="input", default="src/t/pubmed_tina_test.csv",
           help="read data from FILE", metavar="FILE")
                          
        parser.add_option("-d", "--dir", dest="directory", default='output',
            help="write temporary files to DIR (default: 'output/')", metavar="DIR")    
            
        parser.add_option("-o", "--output", dest="output", default='statistics.zip',
            help="zip statistics to FILE (default: statistics.zip)", metavar="FILE")
             
        parser.add_option("-z", "--zip", dest="zip", default='zip',
            help="compression algorithm (default: zip) (no compression: None)", metavar="FILE")
             
        parser.add_option("-c", "--corpora", dest="corpora", default="pubmedTest",
            help="corpora name (default: pubmedTest)", metavar="NAME")     
            
        parser.add_option("-s", "--store", dest="store", default="sqlite://:memory:",
            help="storage engine (default: sqlite://:memory:)", metavar="ENGINE")     
                                      
        parser.add_option("-w", "--stopwords", dest="stopwords", default="src/t/stopwords/en.txt",
            help="loads stopwords from FILE (default: src/t/stopwords/en.txt)", metavar="FILE")                 

        (options, args) = parser.parse_args()
        self.config = options
        
        # try to determine the locale of the current system
        try:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'fr_FR.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        self.stopwords = PyTextMiner.StopWords( "file://%s" % self.config.stopwords )

    def main(self):
        # init of the file reader & parser
        try:
            os.mkdir(self.config.directory)
        except:
            pass
            
        inputpath = "tina://%s"%self.config.input
        tina = Reader(inputpath,
            titleField='doc_titl',
            datetimeField='doc_date',
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
        )
        corpora = PyTextMiner.Corpora( name=self.config.corpora )
        corpora = tina.corpora( corpora )
        #print tina.corpusDict
        #sql = Writer("sqlite://src/t/output/"+corpora.name+".db", locale=self.locale)
        sql = Writer(self.config.store, locale=self.locale)
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
            result = sql.execute(req, [corpusNum])
            dump = Writer ("tina://"+self.config.directory+"/"+corpusNum+"-ngramOccPerCorpus.csv", locale=self.locale)
            # prepares csv export
            filtered = []
            for row in result:
                tag = []
                ng = sql.decode( row[1] )
                for toklist in ng['original']:
                    tag.append( dump.encode( "_".join( toklist ) ) )
                tag = " ".join( tag )
                filtered.append([ ng['str'], row[0], tag ])

            print filtered
            print "writes a corpus-wide ngram synthesis"
            dump.csvFile( ['ngram','documents','POS tagged'], filtered )

            # destroys Corpus object
            del corpus
            del tina.corpusDict[ corpusNum ]
        print "Dumping SQL db"
        sql.dump( self.config.directory+"/dump_"+corpora.name+".sql" )
        
        print "export a sample from document db"
        reader = PyTextMiner.Importer()
        ngramFetch = 'SELECT ng.blob FROM NGram as ng JOIN AssocNGram as asn ON asn.id1 = ng.id WHERE ( asn.id2 = ? )'
        select = 'SELECT doc.id, doc.blob from Document as doc LIMIT 10'
        result = sql.execute(select)
        docList = []
        
        for document in result:
            docid = document[0]
            doc = sql.decode( document[1] )
            ngFetch = sql.execute(ngramFetch, [docid])
            ngList = []
            for ng in ngFetch:
                ngObj = sql.decode( ng[0] )
                ngList += [ ngObj['str'] ]
            doc.ngrams = ", ".join(ngList)
            docList += [doc]
        import codecs
        file = codecs.open(self.config.directory+"/documentSample.csv", "w", 'utf-8', 'xmlcharrefreplace' )
        for doc in docList:
            file.write( "%s,\"%s\"\n" % (doc.docNum, doc.ngrams ) )
            
        if self.config.zip == "zip":
            print "zipping project files.."
            try:
                os.system("zip -r %s %s"%(self.config.output, self.config.directory))
                print "project successfully zipped to", self.config.output
            except:
                print "couldn't zip project folder '%s'. please do it manually" % self.config.directory
if __name__ == '__main__':
    program = Program()
    program.main()
