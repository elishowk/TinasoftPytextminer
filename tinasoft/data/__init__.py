# -*- coding: utf-8 -*-
__all__ = ['basecsv', 'mozstorage', 'tina', 'tinabsddb','gexf']

import jsonpickle

jsonpickle.load_backend('django.util.simplejson', 'dumps', 'loads', ValueError))
jsonpickle.set_preferred_backend('django.util.simplejson')

import sys

class Handler (object):
    """
        Base class for classes
            tinasoft.data.*.Engine
            tinasoft.data.*.Importer
            tinasoft.data.*.Exporter
        Instanciate these classes using the factories
            Engine()
            Writer()
            Reader()
    """

    options = { 'compression' : None,
                'encoding'  : 'utf-8',
                'locale'    : 'en_US.UTF-8'}

    def loadOptions(self, options):
        self.options.update(options)
        for attr, value in self.options.iteritems():
            setattr(self,attr,value)

    def encode(self, toEncode):
        return toEncode.encode( self.encoding, 'replace')

    def decode(self, toDecode):
        return unicode( toDecode, self.encoding, 'replace' )

    def serialize(self, obj):
        return jsonpickle.encode(obj)

    def deserialize(self, str):
        obj= jsonpickle.decode(str)
        return obj

class Importer(Handler):
    pass

class Exporter(Handler):
    pass

# Factories

def _check_protocol(arg):
    protocol, path = arg.split("://")
    return protocol, path

def _factory(arg):
    protocol, path = _check_protocol(arg)
    try:
        name = 'tinasoft.data.'+protocol
        mod = __import__(name)
        obj = sys.modules[name]
        return obj, path
    except ImportError, exc:
        raise Exception("couldn't find module %s: %s"%(protocol,exc))

def Engine(arg, **options):
    module, path = _factory(arg)
    return module.Engine(path, **options)

def Reader(arg, **options):
    module, path = _factory(arg)
    return module.Importer(path, **options)

def Writer(arg, **options):
    module, path = _factory(arg)
    return module.Exporter(path, **options)
