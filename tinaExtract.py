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
from PyTextMiner.Data import Reader, Writer, sqlite, tina
import yaml
import codecs

class Program:
    def __init__(self):
    
        parser = OptionParser()
        
        parser.add_option("-i", "--input", dest="input", default="src/t/pubmed_tina_test.csv",
           help="read data from FILE", metavar="FILE")
                          
        parser.add_option("-d", "--dir", dest="directory", default='output',
            help="write output files to DIR (default: 'output/')", metavar="DIR")    
            
        parser.add_option("-o", "--output", dest="output", default='statistics.zip',
            help="zip statistics to FILE (default: statistics.zip)", metavar="FILE")
             
        parser.add_option("-z", "--zip", dest="zip", default='zip',
            help="compression algorithm (default: zip) (no compression: None)", metavar="FILE")
             
        parser.add_option("-n", "--name", dest="corpora", default="TINA",
            help="corpora name (default: TINA)", metavar="NAME")     
            
        parser.add_option("-s", "--store", dest="store", default="sqlite://output/out.db",
            help="storage engine (default: sqlite://output/out.db)", metavar="ENGINE")     
                                      
        parser.add_option("-w", "--stopwords", dest="stopwords", default="src/t/stopwords/en.txt",
            help="loads stopwords from FILE (default: src/t/stopwords/en.txt)", metavar="FILE")                 

        parser.add_option("-c", "--config", dest="configFile", default="config.yaml",
            help="loads configuration from FILE (default: config.yaml)", metavar="FILE")                 
        
        (options, args) = parser.parse_args()
        self.config = options

        try:
            self.configFile = yaml.safe_load( file( self.config.configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            print "\n Unable to read Config YAML file \n", exc
        #print yaml.safe_dump(self.configFile, allow_unicode=True)

        # try to determine the locale of the current system
        try:
            self.locale = self.configFile['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            print "locale %s was not found, switching to en_US.UTF-8 by default", self.configFile['locale']
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
        #tina = tina.Importer(self.config.input,
            minSize = self.configFile['minSize'],
            maxSize = self.configFile['maxSize'],
            delimiter = self.configFile['delimiter'],
            quotechar = self.configFile['quotechar'],
            locale = self.configFile['locale'],
            fields = self.configFile['fields']
        )
        corpora = PyTextMiner.Corpora( name=self.config.corpora )
        corpora = tina.corpora( corpora )
        # print tina.corpusDict
        sql = Writer(self.config.store, locale=self.locale, format="json")
        # clean the DB contents
        sql.clear()
        # stores Corpora
        if not sql.loadCorpora( corpora.name ) :
            sql.storeCorpora( corpora.name, corpora )
        for corpusNum in corpora.corpora:
            corpusngrams = {}
            # get the Corpus object
            corpus = tina.corpusDict[ corpusNum ]
            # Documents processing
            for documentNum in corpus.documents:
                docngrams = {}
                # only process document if unavailable in DB
                if not sql.loadDocument( documentNum ):
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
                    
                    docngrams = PyTextMiner.Tokenizer.TreeBankWordTokenizer.ngramize(
                        minSize = document.ngramMin,
                        maxSize = document.ngramMax,
                        tokens = document.tokens,
                        emptyString = document.ngramEmpty, 
                        stopwords = self.stopwords,
                    )
                    #sql.storemanyNGram( docngrams.values() )
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
                    sql.storemanyAssocNGramDocument( assocDocIter )
                    # clean full text before DB storage
                    document.rawContent = ""
                    document.tokens = []
                    document.targets = []
                    sql.storeDocument( documentNum, document )
                    del document
                    del tina.docDict[ documentNum ]
                # anyway, insert a new Doc-Corpus association
                sql.storeAssocDocument( documentNum, corpusNum )
                # TODO modulo 10 docs
                print ">> %d documents left to analyse\n" % len( tina.docDict )
                del docngrams
            # end of document extraction
            # clean NGrams
            ( corpusngrams, delList, assocNgramIter ) = PyTextMiner.NGramHelpers.filterUnique( corpusngrams, 2, corpusNum, sql.encode )
            # stores the corpus, ngrams and corpus-ngrams associations
            sql.storemanyNGram( corpusngrams.items() )
            #print corpusngrams.values()
            #assocNgramIter = [( ngid, int(corpusNum), corpusngrams[ ngid ]['occs'] ) for ngid in corpusngrams.iterkeys()]
            sql.storemanyAssocNGramCorpus( assocNgramIter )
            sql.storeCorpus( corpusNum, corpus )
            sql.storeAssocCorpus( corpusNum, corpora.name )
            # removes inexistant ngram-document associations
            sql.cleanAssocNGramDocument( corpusNum )
            # EXPORT ngrams of current corpus FROM DATABASE
            dump = Writer ("tina://"+self.config.directory+"/"+corpusNum+"-ngramOccPerCorpus.csv", locale=self.locale)
            ngrams = sql.fetchCorpusNGram( corpusNum )
            # prepares csv export
            rows = []
            for ngid, ng in ngrams:
                tag=[]
                for tok_tag in ng['original']:
                    tag.append( dump.encode( "_".join( reversed(tok_tag) ) ) )
                tag = " ".join( tag )
                rows.append([ ngid, ng['str'], ng['occs'], tag ])

            # print rows to file
            print "Writing a corpus ngrams summary", corpusNum
            dump.csvFile( ['db id','ngram','documents occs','POS tagged'], rows )
            del ngrams
            # destroys Corpus object
            del corpusngrams
            del corpus
            del tina.corpusDict[ corpusNum ]
        #print "Saving SQL db"
        #sql.dump( self.config.directory+"/dump_"+corpora.name+".sql" )
        
        print "Exporting a sample from document db"
        select = 'SELECT assoc.id1, doc.blob from AssocDocument as assoc JOIN Document as doc ON doc.id = assoc.id1 LIMIT 10'
        result = sql.execute(select).fetchall()

        # format data
        docList = [] 
        for id in result:
            doc = sql.decode(id[1])
            ngrams = sql.fetchDocumentNGram( id[0] )
            ngList = []
            for ng in ngrams:
                ngList += [ ng['str'] ]
            doc['ngrams'] = ", ".join(ngList)
            docList += [doc]

        import codecs
        file = codecs.open(self.config.directory+"/documentSample.csv", "w", 'utf-8', 'replace' )
        file.write( "document_id,document_title,ngrams extracted\n" )
        for doc in docList:
            file.write( "%s,\"%s,\"%s\"\n" % (doc['docNum'], doc['titleField'], doc['ngrams'] ) )
        del docList    
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
