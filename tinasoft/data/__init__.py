#  Copyright (C) 2010 elishowk
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.


__all__ = ['basecsv','whitelist','tinacsv','tinabsddb','gexf','medline','coocmatrix']

# changing jsonpickle serializer
#jsonpickle.load_backend('django.util.simplejson', 'dumps', 'loads', ValueError))
#jsonpickle.set_preferred_backend('django.util.simplejson')

# used by the _factory()
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

    options = {
        'encoding'  : 'utf-8',
    }

    def loadOptions(self, options):
        self.options.update(options)
        for attr, value in self.options.iteritems():
            setattr(self,attr,value)

    def encode(self, toEncode):
        return toEncode.encode( self.encoding, 'replace')

    def decode(self, toDecode):
        return unicode( toDecode, self.encoding, 'replace' )

#class Importer(Handler): pass

#class Exporter(Handler): pass

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
        raise Exception("couldn't  module %s: %s"%(protocol,exc))

def Engine(arg, **options):
    module, path = _factory(arg)
    return module.Engine(path, **options)

def Reader(arg, **options):
    module, path = _factory(arg)
    return module.Importer(path, **options)

def Writer(arg, **options):
    module, path = _factory(arg)
    return module.Exporter(path, **options)
