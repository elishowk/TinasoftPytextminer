# -*- coding: utf-8 -*-

from datetime import datetime
import codecs
import PyTextMiner

class Handler (object):
    options = { 'compression' : None,
                'encoding' : 'utf-8' }
                
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def encode(self, toEncode):
        return toEncode.encode( self.encoding, 'replace')

    def decode(self, toDecode):
        return unicode( toDecode, self.encoding, 'replace' )

class Importer (Handler):
    pass

class Exporter (Handler):
    pass

def _check_protocol(arg):
    protocol, path = arg.split("://")
    return protocol, path

# Engine Factory
def Engine(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        module = __import__("PyTextMiner.Data.%s"%protocol)
        engine = None
        exec ("engine = PyTextMiner.Data.%s.Engine(path, **options)"%protocol)
        return engine
    except Exception, exc:
        raise Exception("couldn't load engine %s: %s"%(protocol,exc))

# Reader Factory
def Reader(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        module = __import__("PyTextMiner.Data.%s"%protocol)
        importer = None
        exec ("importer = PyTextMiner.Data.%s.Importer(path, **options)"%protocol)
        return importer
    except Exception, exc:
        raise Exception("couldn't load reader %s: %s"%(protocol,exc))
         
# Writer Factory
def Writer(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        module = __import__("PyTextMiner.Data.%s"%protocol)
        exporter = None
        exec ("exporter = PyTextMiner.Data.%s.Exporter(path, **options)"%protocol)
        return exporter
    except Exception, exc:
        raise Exception("couldn't load writer %s: %s"%(protocol,exc))
