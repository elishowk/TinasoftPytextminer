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
__author__="elishowk@nonutc.fr"

from tinasoft import TinaApp

from twisted.web import server, resource
from twisted.internet import reactor
from twisted.web.static import File
from twisted.web.resource import NoResource

from os.path import join
import cgi

class TinaServerResource(resource.Resource):
    """
    Request handler
    """
    def __init__(self, method, tinaappsingleton):
        self.method = method
        self.tinaappsingleton = tinaappsingleton
        self.tinaappcallback = TinaServerCallback(
            self.tinaappsingleton.serialize,
            self.tinaappsingleton.deserialize
        )
        # overwriting notify()
        # TinaApp.notify = self.logger
        resource.Resource.__init__(self)

    def render_GET(self, request):
        return self.tinaappsingleton.serialize( self.method(request.args) )

    def render_POST(self):
        print request.args
        return self.tinaappsingleton.serialize( self.method(request.args) )

class TinaServer(resource.Resource):
    """
    Server main class
    dynamically dispatching URL to TinaServerResource() class
    """
    def __init__(self, tinaappsingleton):
        self.tinaappsingleton = tinaappsingleton
        resource.Resource.__init__(self)
    def getChild(self, name, request):
        try:
            method = getattr(self.tinaappsingleton, name)
        except:
            return NoResource()
        else:
            return TinaServerResource(method, self.tinaappsingleton)

class TinaServerCallback():
    """
    Tinaserver's callback class
    """
    def __init__(self, serialize, deserialize):
        self.serialize = serialize
        self.deserialize = deserialize

    def callback(self, msg, returnValue):
        #_observerProxy.notifyObservers(None, msg, jsonpickle.encode( returnValue ))
        return self.serialize( returnValue )

    def import_file( self, returnValue):
        return self.callback( "tinasoft_import_file_finish_status", returnValue)

    def process_cooc( self, returnValue ):
        return self.callback( "tinasoft_process_cooc_finish_status", returnValue)

    def export_whitelist( self, returnValue ):
        return self.callback( "tinasoft_export_whitelist_finish_status", returnValue)

    def export_graph( self, returnValue ):
        return self.callback( "tinasoft_export_graph_finish_status", returnValue)

    def import_whitelist( self, returnValue ):
        return self.callback( "tinasoft_import_whitelist_finish_status", returnValue)

    def get_dataset( self, returnValue ):
        return self.callback( "tinasoft_get_dataset_finish_status", returnValue)

    def get_corpus( self, returnValue ):
        return self.callback( "tinasoft_get_corpus_finish_status", returnValue)

    def get_document( self, returnValue ):
        return self.callback( "tinasoft_get_document_finish_status", returnValue)

    def get_ngram( self, returnValue ):
        return self.callback( "tinasoft_get_ngram_finish_status", returnValue)

    def get_datasets( self, returnValue ):
        return self.callback( "tinasoft_get_datasets_finish_status", returnValue)


if __name__ == "__main__":
    tinaappsingleton = TinaApp()
    tinaserver = TinaServer(tinaappsingleton)
    site = server.Site(tinaserver)
    reactor.listenTCP(8089, site)
    reactor.run()
