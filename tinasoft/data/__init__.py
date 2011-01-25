# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
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


__all__ = ['tinasqlite','basecsv','whitelist','tinacsv','source','gexf','medline','medlinearchive','coocmatrix']

# changing jsonpickle serializer
#jsonpickle.load_backend('django.util.simplejson', 'dumps', 'loads', ValueError))
#jsonpickle.set_preferred_backend('django.util.simplejson')

# used by the _factory()
import sys
import codecs

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

    path = None
    file = None
    # defaults
    options = {
        'encoding': 'utf_8'
    }
    
    #def __del__(self):
    #    self.file.close()
    
    def loadOptions(self, options):
        self.options.update(options)
        for attr, value in self.options.iteritems():
            setattr(self,attr,value)

    def encode(self, toEncode):
        return toEncode.encode( self.encoding, 'ignore')

    def unicode(self, toDecode):
        return unicode( toDecode, self.encoding, 'ignore' )
        
    def _coerce_unicode(self, cell):
        """
        checks a value and eventually converts to unicode
        """
        if type(cell) != unicode:
            return self.unicode(cell)
        else:
            return cell

class Importer(Handler):
    
    def __init__(self, path, **kwargs):
        self.path = path
        self.loadOptions( kwargs )
        self.file = self.open(path)
        self.line_num = 0

    def open(self, path):
        return codecs.open( path, 'rU', encoding=self.encoding, errors='ignore')
        
class Exporter(Handler): pass

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
        raise Exception("couldn't load module %s: %s"%(protocol,exc))

def Engine(arg, **options):
    module, path = _factory(arg)
    return module.Engine(path, **options)

def Reader(arg, **options):
    module, path = _factory(arg)
    return module.Importer(path, **options)

def Writer(arg, **options):
    module, path = _factory(arg)
    return module.Exporter(path, **options)
