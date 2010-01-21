# -*- coding: utf-8 -*-
__all__ = ['basecsv', 'mozstorage', 'sqlite', 'sqlapi', 'tina', 'tinabsddb']

import jsonpickle

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
                'encoding' : 'utf-8' }

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
        #print "deserialized object", obj
        return obj

class Importer(Handler):
    pass

class Exporter(Handler):
    pass

# Factories

def _check_protocol(arg):
    protocol, path = arg.split("://")
    return protocol, path

def Engine(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        exec("import %s as module"%protocol)
        return module.Engine(path, **options)
    except Exception, exc:
        raise Exception("couldn't load engine %s: %s"%(protocol,exc))

def Reader(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        exec("import %s as module"%protocol)
        return module.Importer(path, **options)
    except Exception, exc:
        raise Exception("couldn't load reader %s: %s"%(protocol,exc))

def Writer(arg, **options):
    protocol, path = _check_protocol(arg)
    try:
        exec("import %s as module"%protocol)
        return module.Exporter(path, **options)
    except Exception, exc:
        raise Exception("couldn't load writer %s: %s"%(protocol,exc))
