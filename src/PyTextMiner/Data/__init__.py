# -*- coding: utf-8 -*-

from datetime import datetime
import codecs
import PyTextMiner

class Handler (object):
    options = { 'compression' : None,
                'encoding' : 'utf-8' }
                
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding
  
    def load_options(self, options):
        for attr, default in self.options.iteritems():
            try:
                self.__setattr__(attr,options[attr])
            except:
                self.__setattr__(attr,default)
                
    def encode(self, toEncode):
        return toEncode.encode( self.encoding, 'xmlcharrefreplace')

    def decode(self, toDecode):    
        return unicode( toDecode, self.encoding, 'xmlcharrefreplace' )

class Importer (Handler):
    pass

class Exporter (Handler):
    pass

def _check_protocol(arg):
    protocol, path = arg.split("://")
    return protocol, path
    
def Reader(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        module = __import__("PyTextMiner.Data.%s"%protocol)
        importer = None
        #exec "from "PyTextMiner.Data.%s"%protocol import module.
        exec ("importer = PyTextMiner.Data.%s.Importer(path, **options)"%protocol)
        return importer
    except Exception, exc:
        raise Exception("couldn't load reader %s: %s"%(protocol,exc))
         
def Writer(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        module = __import__("PyTextMiner.Data.%s"%protocol)
        exporter = None
        #exec "from "PyTextMiner.Data.%s"%protocol import module.
        exec ("exporter = PyTextMiner.Data.%s.Exporter(path, **options)"%protocol)
        return exporter
    except Exception, exc:
        raise Exception("couldn't load writer %s: %s"%(protocol,exc))

def Encode(str):
    return codecs.encode( str, enc, 'replace' )
