# -*- coding: utf-8 -*-

from datetime import datetime
import codecs
import PyTextMiner

class Importer (object):
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def decode(self, toDecode):
        # replacement char = \ufffd        
        return unicode( toDecode, self.encoding, 'xmlcharrefreplace' )

    def get_property(self, options, key, default):
        if not options.has_key(key): 
            options[key] = default
        return options[key]

class Exporter (object):
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def encode(self, toEncode):
        # replacement utf-8 char = \xef\xbf\xbd
        return toEncode.encode( self.encoding, 'xmlcharrefreplace')

    def get_property(self, options, key, default):
        if not options.has_key(key): 
            options[key] = default
        return options[key]

def _check_protocol(arg):
    protocol, path = arg.split("://")
#    if protocol == "file+medline" or protocol == "medline":
#        protocol = "medline"
#    elif protocol == "file+tina" or protocol == "tina":
#        protocol = "fet"
#    elif protocol == "csv":
#        protocol = "basecsv"
#    elif protocol == "sqlite" or protocol == "sql":
#        protocol = "sqlite"
#    else:
#        raise Exception("unrecognized protocol %s"%protocol)
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
