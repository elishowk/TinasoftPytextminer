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

tinaappsingleton = TinaApp()

class TinaServerResource(resource.Resource):
    #isLeaf = True
    def __init__(self, method):
        self.method = method
        #TinaApp.__init__(self)
        #self.callback = TinaServerCallback()
        # overwriting notify()
        # TinaApp.notify = self.logger
        resource.Resource.__init__(self)

    def render_GET(self, request):
        return tinaappsingleton.serialize( self.method(request) )

    #def render_POST(self):
    #   request.finish()

class TinaServer(resource.Resource):
    def getChild(self, name, request):
        try:
            method = getattr(tinaappsingleton, name)
        except:
            return NoResource()
        else:
            return TinaServerResource(method)

class TinaServerCallback():
    """
    Tinaserver's callback class
    """

    def callback(self, msg, returnValue):
        #_observerProxy.notifyObservers(None, msg, jsonpickle.encode( returnValue ))
        return jsonpickle.encode( returnValue )

    def importFile( self, returnValue):
        return self.callback( "tinasoft_runImportFile_finish_status", returnValue)

    def processCoocGraph( self, returnValue ):
        return self.callback( "tinasoft_runProcessCoocGraph_finish_status", returnValue)

    def exportCorpora( self, returnValue ):
        return self.callback( "tinasoft_runExportCorpora_finish_status", returnValue)

    def exportGraph( self, returnValue ):
        return self.callback( "tinasoft_runExportGraph_finish_status", returnValue)

if __name__ == "__main__":
    tinaserver = TinaServer()
    site = server.Site(tinaserver)
    reactor.listenTCP(8089, site)
    reactor.run()
