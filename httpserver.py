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
import traceback
# unescaping uri components
import cgi
from os.path import join

class TinaServerResource(resource.Resource):
    """
    Request handler
    """
    argument_types = {
        'index' : bool,
        'overwrite': bool,
        'path': str,
        'outpath': str,
        'dataset': str,
        'label':str,
        'periods': list,
        'threshold': list,
        'whitelist': str,
        'whitelistlabel': str,
        'userstopwords': str,
        'ngramlimit': int,
        'minoccs' : int,
        'id': str,
        'format': str,
        'filetype': str,
    }
    def __init__(self, method, back):
        self.method = method
        self.back = back
        # overwriting notify()
        # TinaApp.notify = self.logger
        resource.Resource.__init__(self)

    def render(self, request):
        """
        Prepares arguments and call the method
        """
        parsed_args = {}
        for key in request.args.iterkeys():
            if key not in self.argument_types:
                continue
            if self.argument_types[key] == '':
                parsed_args[key] = None
                continue
            if self.argument_types[key] != list:
                parsed_args[key] = self.argument_types[key](request.args[key][0])
            else:
                parsed_args[key] = self.argument_types[key](request.args[key])
        print self.method, parsed_args
        if 'whitelist' in parsed_args and parsed_args['whitelist'] is not None:
            parsed_args['whitelist'] = TinaApp.import_whitelist(parsed_args['whitelist'],'')
        if 'userstopwords' in parsed_args and parsed_args['userstopwords'] is not None:
            parsed_args['userstopwords'] = TinaApp.import_userstopwords(parsed_args['userstopwords'])
        try:
            return self.back.call( self.method(**parsed_args) )
        except Exception, e:
            return ErrorPage(500, "tinaserver error, please report the following message to elias.showk@iscpif.fr", traceback.format_exc() ).render(request)

class TinaServer(resource.Resource):
    """
    Server main class
    dynamically dispatching URL to TinaServerResource() class
    """
    def __init__(self, tinacallback, posthandler, gethandler):
        self.callback = tinacallback
        self.posthandler = posthandler
        self.gethandler = gethandler
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        try:
            if request.method == 'POST':
                method = getattr(self.posthandler, name)
            elif request.method == 'GET':
                method = getattr(self.gethandler, name)
            else:
                raise Exception()
        except:
            return NoResource()
        else:
            return TinaServerResource(method, self.callback)


class TinaAppPOST():
    """
    TinaApp mapping POST requests on an instance of TinaApp's
    """
    def __init__(self, tinaappinstance):
        self.tinaappinstance = tinaappinstance

    def file(self, *args, **kwargs):
        # import file
        return self.tinaappinstance.import_file(*args, **kwargs)

    #def whitelist(self, *args, **kwargs):
        # import whitelist
    #    return TinaApp.import_whitelist(*args, **kwargs)

    def cooccurrences(self, *args, **kwargs):
        # process_cooc
        return self.tinaappinstance.process_cooc(*args, **kwargs)

    def graph(self, *args, **kwargs):
        # export_graph
        return self.tinaappinstance.export_graph(*args, **kwargs)

    def dataset(self, corporaobj):
        # insert
        self.tinaappinstance.set_storage( corporaobj['id'] )
        return self.tinaappinstance.storage.insertCorpora(corporaobj)

    def corpus(self, dataset, corpusobj):
        # insert
        self.tinaappinstance.set_storage( dataset )
        return self.tinaappinstance.storage.insertCorpus(corpusobj)

    def document(self, dataset, documentobj):
        self.tinaappinstance.set_storage( dataset )
        return self.tinaappinstance.storage.insertDocument(documentobj)

    def ngram(self, dataset, ngramobj):
        self.tinaappinstance.set_storage( dataset )
        return self.tinaappinstance.storage.insertNgram(ngramobj)


class TinaAppGET():
    """
    TinaApp mapping GET requests on an instance of TinaApp's
    """
    def __init__(self, tinaappinstance):
        self.tinaappinstance = tinaappinstance

    def file(self, *args, **kwargs):
        """
        runs an extraction process and exports
        a whitelist csv file for a given dataset and source file
        """
        return self.tinaappinstance.extract_file(*args, **kwargs)

    def whitelist(self, *args, **kwargs):
        """exports a whitelist csv file for a given dataset, periods, and whitelist"""
        return self.tinaappinstance.export_whitelist(*args, **kwargs)

    def cooccurrences(self, *args, **kwargs):
        """exports a cooc matrix for a given datasset, periods, and whitelist"""
        return self.tinaappinstance.export_cooc(*args, **kwargs)

    def graph(self, *args, **kwargs):
        """list the existing graphs for a given dataset"""
        return self.tinaappinstance.walk_user_path(*args, **kwargs)

    def dataset(self, dataset):
        # load
        self.tinaappinstance.set_storage(dataset)
        return self.tinaappinstance.storage.loadCorpora(dataset)

    def corpus(self, dataset, id):
        self.tinaappinstance.set_storage( dataset )
        return self.tinaappinstance.storage.loadCorpus(id)

    def document(self, dataset, id):
        self.tinaappinstance.set_storage( dataset )
        return self.tinaappinstance.storage.loadDocument(id)

    def ngram(self, dataset, id):
        self.tinaappinstance.set_storage( dataset )
        return self.tinaappinstance.storage.loadNGram(id)


class TinaServerCallback():
    """
    Tinaserver's callback class
    """
    default = ""

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
            returnValue = self.default
        return self.serialize( returnValue )


if __name__ == "__main__":
    tinaappsingleton = TinaApp()
    posthandler = TinaAppPOST(tinaappsingleton)
    gethandler = TinaAppGET(tinaappsingleton)
    tinacallback = TinaServerCallback()
    tinaserver = TinaServer(tinacallback, posthandler, gethandler)
    tinaserver.putChild("user",
        File(join( tinaappsingleton.user ))
    )
    site = server.Site(tinaserver)
    reactor.listenTCP(8888, site)
    reactor.run()
