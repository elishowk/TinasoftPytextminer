#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import os
import locale
from optparse import OptionParser

# initialize the system path with local dependencies and pre-built libraries
import bootstrap

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Reader, Writer, tina
import yaml

class Program:
    def __init__(self):

        # import config yaml
        try:
            self.config = yaml.safe_load( file( "config.yaml", 'rU' ) )
        except yaml.YAMLError, exc:
            print "\nUnable to read ./config.yaml file : ", exc
            return

        # command-line parser
        parser = OptionParser()
        parser.add_option("-i", "--input", dest="input", default=self.config['input'], help="read input data from FILE, default: "+self.config['input'], metavar="FILE")
               
        parser.add_option("-d", "--dir", dest="directory", default=self.config['directory'], help="write output files to DIR, default: "+self.config['directory'], metavar="DIR")

        parser.add_option("-o", "--output", dest="output", default=self.config['output'],
            help="compress output to FILE, default: "+self.config['output'], metavar="FILE")

        parser.add_option("-z", "--zip", dest="zip", default=self.config['zip'], help="compression algorithm (no compression: None), default: "+self.config['zip'], metavar="FILE")

        parser.add_option("-n", "--name", dest="corpora", default=self.config['name'], help="Corpora identifier or name, default: "+self.config['name'], metavar="NAME")

        parser.add_option("-s", "--store", dest="store", default=self.config['store'], help="storage engine, default: "+self.config['store'], metavar="ENGINE")
         
        parser.add_option("-w", "--stopwords", dest="stopwords", default=self.config['stopwords'], help="loads stopwords from FILE, default: "+self.config['stopwords'], metavar="FILE")

        parser.add_option("-m", "--min", dest="minSize", default=self.config['minSize'], help="n-gram minimum size extraction, default: "+ str(self.config['minSize']))

        parser.add_option("-x", "--max", dest="maxSize", default=self.config['maxSize'], help="n-gram maximum size extraction, default: "+ str(self.config['maxSize']))

        parser.add_option("-l", "--locale", dest="locale", default=self.config['locale'], help="Locale (text encoding), default: "+self.config['locale'])

        (cmdoptions, args) = parser.parse_args()
        self.options = cmdoptions
        # tries support of the locale by the host system
        try:
            self.locale = self.options.locale
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            print "locale %s was not found, switching to en_US.UTF-8 by default", self.options.locale
            locale.setlocale(locale.LC_ALL, self.locale)
        # load Stopwords object
        self.stopwords = PyTextMiner.StopWords( "file://%s" % self.options.stopwords )
        # format sqlite dbi string
        if not self.options.store == ":memory:":
            self.options.store = "tina://"+self.options.directory+"/"+self.options.store
        else:     
            self.options.store = "tina://"+self.options.store
        # init of the file reader & parser
        try:
            os.mkdir(self.options.directory)
        except:
            pass

    def main(self):

        inputpath = "tina://%s"%self.options.input
        tinaImporter = Reader(inputpath,
            minSize = self.options.minSize,
            maxSize = self.options.maxSize,
            delimiter = self.config['delimiter'],
            quotechar = self.config['quotechar'],
            locale = self.locale,
            fields = self.config['fields']
        )
        corpora = PyTextMiner.Corpora( name=self.options.corpora )
        corpora = tinaImporter.corpora( corpora )
        # print tinaImporter.corpusDict
        tinasqlite = Writer(self.options.store, locale=self.locale, format="json")
        # clean the DB contents
        tinasqlite.clear()
        # stores Corpora
        tinasqlite.storeCorpora( corpora.name, corpora )
        for corpusNum in corpora.corpora:

            # get the Corpus object and import
            corpus = tinaImporter.corpusDict[ corpusNum ]
            tinaImporter.docDict = tinasqlite.importCorpus( corpus, corpusNum, PyTextMiner.Tokenizer.TreeBankWordTokenizer, PyTextMiner.Tagger.TreeBankPosTagger, self.stopwords, corpora, tinaImporter.docDict )
            # EXPORT ngrams of current corpus FROM DATABASE
            csvdumper = Writer ("basecsv://"+self.options.directory+"/"+corpusNum+"-ngramOccPerCorpus.csv", locale=self.locale)
            ngrams = tinasqlite.fetchCorpusNGram( corpusNum )
            # prepares csv export
            rows = []
            for ngid, ng in ngrams:
                tag=[]
                for tok_tag in ng['original']:
                    tag.append( csvdumper.encode( "_".join( reversed(tok_tag) ) ) )
                tag = " ".join( tag )
                rows.append([ ngid, ng['str'], ng['occs'], tag ])

            #coword = PyTextMiner.CoWord.SimpleAnalysis()
            #matrix = coword.processCorpus( tinasqlite, self.options.corpus );
            #tinasqlite.insertmanyCooc( matrix.values() )
            # print rows to file
            print "Writing a corpus ngrams summary", corpusNum
            csvdumper.csvFile( ['db id','ngram','documents occs','POS tagged'], rows )
            del rows
            del csvdumper
            del ngrams
            del corpus
            del tinaImporter.corpusDict[ corpusNum ]
        #print "Dumping tinasqlite db"
        #tinasqlite.dump( self.options.directory+"/csvdumper_"+corpora.name+".tinasqlite" )

        # export sample of db
        print "Exporting a sample from document db"
        select = 'SELECT assoc.id1, doc.blob from AssocDocument as assoc JOIN Document as doc ON doc.id = assoc.id1 LIMIT 10'
        result = tinasqlite.execute(select).fetchall()
        docList = [] 
        for id in result:
            doc = tinasqlite.decode(id[1])
            ngrams = tinasqlite.fetchDocumentNGram( id[0] )
            ngList = []
            for ng in ngrams:
                ngList += [ ng['str'] ]
            doc['ngrams'] = ", ".join(ngList)
            docList += [doc]
        docSampleDumper = Writer ("basecsv://"+self.options.directory+"/documentSample.csv", locale=self.locale)
        rows = []
        for doc in docList:
            rows.append([ doc['docNum'], doc['titleField'], doc['ngrams']  ])
        del docList
        docSampleDumper.csvFile( ['doc id','doc_title','extracted ngrams'], rows )
        # zip the output directory
        if self.options.zip == "zip":
            print "zipping project files.."
            try:
                os.system("zip -r %s %s"%(self.options.output, self.options.directory))
                print "project successfully zipped to", self.options.output
            except:
                print "couldn't zip project folder '%s'. please do it manually" % self.options.directory

if __name__ == '__main__':
    program = Program()
    program.main()
