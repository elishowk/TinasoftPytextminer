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
# error handling
from twisted.web.resource import NoResource, ErrorPage

# json encoder for communicate with the outer world
import jsonpickle
# unescaping uri components
#import cgi
from os.path import join

class TinaServerResource(resource.Resource):
    """
    Request handler
    """
    def __init__(self, method, back):
        self.method = method
        self.back = back
        # overwriting notify()
        # TinaApp.notify = self.logger
        resource.Resource.__init__(self)

    def render(self, request):
        print request.method, request.args
        try:
            return self.back.call( self.method(request.args) )
        except:
            return ErrorPage(500, "error", "").render(request)

class TinaServer(resource.Resource):
    """
    Server main class
    dynamically dispatching URL to TinaServerResource() class
    """
    def __init__(self, tinacallback, tinaappsingletonGET, tinaappsingletonPOST):
        self.callback = tinacallback
        self.tinaappsingletonPOST = tinaappsingletonPOST
        self.tinaappsingletonGET = tinaappsingletonGET
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        try:
            if request.method == 'POST':
                #print name
                method = getattr(self.tinaappsingletonPOST, name)
            elif request.method == 'GET':
                #print name
                method = getattr(self.tinaappsingletonGET, name)
            else:
                raise Exception()
        except:
            return NoResource()
        else:
            return TinaServerResource(method, self.callback)


class TinaAppPOST(TinaApp):
    """
    TinaApp subclass mapping POST request on TinaApp's methods
    """
    def file(self, *args, **kwargs):
        # import file
        return self.import_file(*args, **kwargs)
    def whitelist(self, *args, **kwargs):
        # import whitelist
        return self.import_whitelist(*args, **kwargs)

    def cooccurrences(self, *args, **kwargs):
        # process_cooc
        return self.process(*args, **kwargs)

    def graph(self, *args, **kwargs):
        # export_graph
        return self.export_graph(*args, **kwargs)

    def dataset(self, corporaobj):
        # insert
        if corporaobj['id'] is not None:
            self.set_storage( corporaobj['id'] )
            return self.storage.insertCorpora(corporaobj)

    def corpus(self, corporaid, corpusobj):
        # insert
        if corporaid is not None:
            self.set_storage( corporaid )
            return self.storage.insertCorpus(corpusobj)

    def document(self, corporaid, documentobj):
        if corporaid is not None:
            self.set_storage( corporaid )
            return self.storage.insertDocument(documentobj)

    def ngram(self, corporaid, ngramobj):
        if corporaid is not None:
            self.set_storage( corporaid )
            return self.storage.insertNgram(ngramobj)


class TinaAppGET(TinaApp):
    """
    TinaApp subclass mapping GET request on TinaApp's methods
    """
    def file(self, *args, **kwargs):
        # extract_file
        return self.extract_file(*args, **kwargs)

    def whitelist(self, *args, **kwargs):
        #export_whitelist
        return self.export_whitelist(*args, **kwargs)

    def cooccurrences(self, *args, **kwargs):
        # export_cooc
        return self.export_cooc(*args, **kwargs)

    def graph(self, *args, **kwargs):
        # list of graphs for a given corpora id
        return self.walk_graph_path(*args, **kwargs)

    def dataset(self, corporaid):
        # load
        if corporaid is not None:
            self.set_storage( corporaid )
            return self.storage.loadCorpora(corporaid)

    def corpus(self, corporaid, corpusid):
        if corporaid is not None:
            self.set_storage( corporaid )
            return self.storage.loadCorpus(corpusid)

    def document(self, corporaid, documentid):
        if corporaid is not None:
            self.set_storage( corporaid )
            return self.storage.loadDocument(documentid)

    def ngram(self, corporaid, ngramid):
        if corporaid is not None:
            self.set_storage( corporaid )
            return self.storage.loadNgram(ngramid)


class TinaServerCallback():
    """
    Tinaserver's callback class
    """

    def serialize(self, obj):
        """
        Encoder to send messages to the host appllication
        """
        return jsonpickle.encode(obj)

    def deserialize(self, str):
        """
        Decoder for the host's application messages
        """
        return jsonpickle.decode(str)

    def call(self, returnValue=None):
        #_observerProxy.notifyObservers(None, msg, jsonpickle.encode( returnValue ))
        if returnValue == None:
            returnValue = ""
        return self.serialize( returnValue )


if __name__ == "__main__":
    tinaappsingletonPOST = TinaAppPOST()
    tinaappsingletonGET = TinaAppGET()
    tinacallback = TinaServerCallback()
    tinaserver = TinaServer(tinacallback, tinaappsingletonPOST, tinaappsingletonGET)
    tinaserver.putChild("user", File(join( tinaappsingletonGET.config['general']['basedirectory'], tinaappsingletonGET.config['general']['user'] )))
    site = server.Site(tinaserver)
    reactor.listenTCP(88888, site)
    reactor.run()
