# -*- coding: utf-8 -*-

from datetime import datetime
import codecs
import PyTextMiner

class Importer (object):
    def __init__(self):
        pass

class Exporter (object):
    def __init__(self):
        pass

 
def _get_locale(locale='en_US:UTF-8'):
    try:
        lang, encoding = locale.split(':') 
        return lang, encoding.lower()
    except:
        return locale.split(':')[0], 'utf-8'

def _check_protocol(arg):
    protocol, path = arg.split("://")
    if protocol == "file+medline" or protocol == "medline":
        protocol = "medline"
    elif protocol == "file+fet" or protocol == "fet":
        protocol = "fet"
    elif protocol == "csv":
        protocol = "basecsv"
    else:
        raise Exception("unrecognized protocol %s"%protocol)
    return protocol, path
    
def Reader(arg, **kwargs):
    protocol, path = _check_protocol(arg)
    try:
        module = __import__("PyTextMiner.Data.%s"%protocol)
        importer = None
        #exec "from "PyTextMiner.Data.%s"%protocol import module.
        exec ("importer = PyTextMiner.Data.%s.Importer(path,  **kwargs)"%protocol)
        return importer
    except Exception, exc:
        raise Exception("couldn't load reader %s: %s"%(protocol,exc))
         
def Writer(arg,  *args, **kwargs):
    protocol, path = _check_protocol(arg)
    try:
        module = __import__("PyTextMiner.Data.%s"%protocol)
        exporter = None
        #exec "from "PyTextMiner.Data.%s"%protocol import module.
        exec ("exporter = PyTextMiner.Data.%s.Exporter(path,  *args, **kwargs)"%protocol)
        return exporter
    except Exception, exc:
        raise Exception("couldn't load writer %s: %s"%(protocol,exc))
