#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import os
from optparse import OptionParser
import locale
# initialize the system path with local dependencies and pre-built libraries
import bootstrap

# pytextminer modules
from PyTextMiner.CoWord import *
from PyTextMiner.Data import Engine, tina
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

        parser.add_option("-c", "--corpus", dest="corpus", default=self.config['corpus'], help="Corpus ID to process, default: "+str(self.config['corpus']))
        parser.add_option("-l", "--locale", dest="locale", default=self.config['locale'], help="Locale (text encoding), default: "+self.config['locale'])
        parser.add_option("-s", "--store", dest="store", default=self.config['store'], help="storage engine, default: "+self.config['store'], metavar="ENGINE")
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

        # format sqlite dbi string
        if not self.options.store == ":memory:":
            self.options.store = "sqlite://"+self.options.store
        else:     
            self.options.store = "sqlite://"+self.options.store


    def main(self):
        tinasqlite = Engine( self.options.store, locale=self.locale, format="json")
        coword = ThreadedAnalysis()
        matrix = coword.analyseCorpus( tinasqlite, self.options.corpus, 2 );
        #tinasqlite.insertmanyCooc( matrix.values() )

 

if __name__ == '__main__':
    program = Program()
    program.main()
