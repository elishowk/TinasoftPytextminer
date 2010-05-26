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
__author__="Elias Showk"

import tornado.httpserver
import tornado.ioloop
import tornado.web

from tinasoft import TinaApp

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


class TinaServer(TinaApp, tornado.web.RequestHandler):

    def __init__(self, *args, **kwargs):
        TinaApp.__init__(self)
        self.callback = TinaServerCallback()
        # overwriting notify()
        # TinaApp.notify = self.logger
        self.logger.debug("TinaServer started")
        return tornado.web.RequestHandler.__init__(self, *args, **kwargs)

    def get(self):
        self.write(self.request.arguments)
        self.write(self.request.path)
        self.write(self.request.headers)

    def post(self): pass

    def walk_graph_path( self, corporaid ):
        """returns the list of files in the gexf directory tree"""
        path = join( self.config['user'], corporaid )
        if not exists( path ):
            return self.serialize( [] )
        return self.serialize( [join( path, file ) for file in os.listdir( path )] )

    def _get_graph_path(self, corporaid, periods, threshold=[0.0,1.0]):
        """returns the relative path for a given graph in the graph dir tree"""
        path = join( self.config['user'], corporaid )
        if not exists( path ):
            makedirs( path )
        filename = "-".join( periods ) + "_" \
            + "-".join( map(str,threshold) ) \
            + ".gexf"
        #self.logger.debug( join( path, filename ) )
        return join( path, filename )

    def __del__(self):
        """resumes all the torage transactions when destroying this object"""
        del self.storage


tinaserver = tornado.web.Application([
    (r"/*", TinaServer)
])
if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(tinaserver)
    http_server.listen(9889)
    tornado.ioloop.IOLoop.instance().start()
