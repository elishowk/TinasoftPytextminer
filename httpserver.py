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
#import cgi
from os.path import join
import sys

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
        'whitelistpath': str,
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
        resource.Resource.__init__(self)

    def render(self, request):
        """
        Prepares arguments and call the method
        """
        parsed_args = {}

        # parameters parsing
        for key in request.args.iterkeys():
            if key not in self.argument_types:
                continue
            if self.argument_types[key](request.args[key][0]) == '':
                parsed_args[key] = None
                continue
            if self.argument_types[key] == bool:
                if request.args[key][0] == 'True': parsed_args[key] = True
                if request.args[key][0] == 'False': parsed_args[key] = False
            elif self.argument_types[key] == list:
                parsed_args[key] = self.argument_types[key](request.args[key])
            else:
                parsed_args[key] = self.argument_types[key](request.args[key][0])

        print self.method, parsed_args

        request.setHeader("content-type", "application/json")
        # sends the request through the callback
        try:
            returnValue = self.method(**parsed_args)
            if returnValue == TinaApp.STATUS_ERROR:
                return ErrorPage(500, "tinaserver error, please report it", traceback.format_exc() ).render(request)
            else:
                return self.back.call( returnValue )
        except Exception, e:
            return ErrorPage(500, "tinaserver error, please report it", traceback.format_exc() ).render(request)

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
        # index file
        return self.tinaappinstance.index_file(*args, **kwargs)

    def whitelist(self, *args, **kwargs):
        # import whitelist
        return TinaApp.import_whitelist(*args, **kwargs)

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

    def graph(self, dataset):
        """list the existing graphs for a given dataset"""
        return self.tinaappinstance.walk_user_path(dataset, 'gexf')

    def dataset(self, dataset):
        """
        return a dataset json object from the database
        """
        ### if argument is empty, return the list of all datasets
        if dataset is None:
            return self.tinaappinstance.walk_datasets()

        elif self.tinaappinstance.set_storage(dataset, create=False) == self.tinaappinstance.STATUS_OK:
            return self.tinaappinstance.storage.loadCorpora(dataset)
        else:
            return None

    def corpus(self, dataset, id):
        """
        return a corpus json object from the database
        """
        if self.tinaappinstance.set_storage(dataset, create=False) == self.tinaappinstance.STATUS_OK:
            return self.tinaappinstance.storage.loadCorpus(id)
        else:
            return None

    def document(self, dataset, id):
        """
        return a document json object from the database
        """
        if self.tinaappinstance.set_storage(dataset, create=False) == self.tinaappinstance.STATUS_OK:
            return self.tinaappinstance.storage.loadDocument(id)
        else:
            return None

    def ngram(self, dataset, id):
        """
        return a ngram json object from the database
        """
        if self.tinaappinstance.set_storage(dataset, create=False) == self.tinaappinstance.STATUS_OK:
            return self.tinaappinstance.storage.loadNGram(id)
        else:
            return None

    def walk_user_path(self, dataset, filetype):
        """list any existing fily for a given dataset and filetype"""
        return self.tinaappinstance.walk_user_path(dataset, filetype)

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


def run(customdir=None):
    if customdir is not None:
        tinaappsingleton = TinaApp(homedir=customdir)
    else:
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

if __name__ == "__main__":
    import getopt
    opts, args = getopt.gnu_getopt(sys.argv[1:],'d')
    dir=None
    for o, a in opts:
        if o == "-d":
            dir=a
    run(customdir=dir)
