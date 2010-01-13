# -*- coding: utf-8 -*-
__author__="Elias Showk"
__all__ = ["pytextminer","data"]
# use this to dispatch tinasoft in multiple packages/directory
#__import__('pkg_resources').declare_namespace(__name__)

# core modules
from tinasoft.pytextminer import stopwords, indexer
from tinasoft.data import Engine

# third party utils
import os
import locale
from optparse import OptionParser
import shutil
# configuration file parsing
import yaml

class TinaApp():
    """ base class for a tinasoft.pytextminer application"""
    def __init__(self, configFile='config.yaml', storage=None, loc=None, stopw=None, index=None ):
        # import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            print "\nUnable to read ./config.yaml file : ", exc
            return
        self.storage = storage
        if self.storage is None:
            self.storage = self.config['storage']
        # tries support of the locale by the host system
        try:
            if loc is None:
                self.locale = self.config['locale']
            else:
                self.locale = loc
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            print "locale %s was not found, switching to en_US.UTF-8 by default", self.locale
            locale.setlocale(locale.LC_ALL, self.locale)

        # load Stopwords object
        if stopw is None:
            self.stopwords = stopwords.StopWords( "file://%s" % self.config['stopwords'] )
        else:
            self.stopwords = stopwords.StopWords( "file://%s" % stopw )

        # connect sqlite database
        if storage is None:
            self.storage = Engine("sqlite://"+self.config['storage'])
        else:
            self.storage = Engine("sqlite://"+storage)

        # connect to text-index
        if index is None:
            self.index = indexer.TinaIndex(self.config['index'])
        else:
            self.index = indexer.TinaIndex(index)
