﻿#!/usr/bin/python

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

from twisted.internet.task import cooperate
from twisted.web import server, resource
from twisted.internet import reactor
from twisted.web.static import File
from twisted.internet.protocol import Factory, Protocol
# error handling
from twisted.web.resource import NoResource, ErrorPage

# json encoder to communicate with the outer world
import numpy
import jsonpickle
# traceback to print error traces
import traceback

# parsing uri components
from urlparse import parse_qs
import urllib
# web browser control
import webbrowser

# OS utilities
from os.path import join, exists
import sys
import exceptions

import logging
import tempfile
# MS windows trick to print to a file server's output to a file
import platform
if platform.system() == 'Windows':
    sys.stdout = open('stdout.log', 'a+b')
    sys.stderr = open('stderr.log', 'a+b')


class TinaServerResource(resource.Resource):
    """
    The request handler
    """
    def __init__(self, handler, method, callback, logger):
        """
        to be executed method on an object, its callback and a TinaApp logger connector
        """
        self.handler = handler
        self.method = method
        self.callback = callback
        self.logger = logger
        resource.Resource.__init__(self)

    def _parse_args(self, args):
        """
        Universal request arguments parsing and type convertions
        """
        parsed_args = {}
        # parameters parsing
        for key in args.iterkeys():
            if key not in self.argument_types:
                continue
            # empty args handling
            if args[key][0] == '':
                parsed_args[key] = None
            # boolean args handling
            elif self.argument_types[key] == bool:
                if args[key][0] == 'True': parsed_args[key] = True
                if args[key][0] == 'False': parsed_args[key] = False
            # list args
            elif self.argument_types[key] == list:
                parsed_args[key] = self.argument_types[key](args[key])
            elif self.argument_types[key] == parse_qs:
                parsed_args[key] = self._parse_args( self.argument_types[key](args[key][0]) )
            else:
                parsed_args[key] = self.argument_types[key](args[key][0])
        return parsed_args

    def render(self, request):
        """
        Prepares arguments and call the method
        """
        #self.logger.info( str(self.method) + " ---" + str(parsed_args) )
        d = CooperativeExecution()._method_wrapper(request, self.callback.success, self.handler, self.method)
        d.addCallback(lambda ignored: request.finish())
        d.addErrback(self._method_failed)
        return server.NOT_DONE_YET

    def _method_failed(self, err):
        self.logger.error( str(err) )

class CooperativeExecution(object):
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
        'fileurl': str,
        'ngramgraphconfig': parse_qs,
        'documentgraphconfig': parse_qs,
        'proximity': str,
        'alpha': float,
        'nodethreshold': list,
        'edgethreshold': list,
    }

    def _method_wrapper(self, request, serializer, handler, method):
        self.request = request
        self.serializer = serializer
        self.handler = handler
        self.method = method
        self.request.registerProducer(self, True)
        self._task = cooperate(self._call_method())
        d = self._task.whenDone()
        d.addBoth(self._unregister)
        return d

    def _call_method(self):
        """
        Wrapper of any API call to fit asynchronous server behaviour
        """
        parsed_args = self._parse_args(self.request.args)
        methodfunction = getattr(self.handler, self.method)
        self.request.setHeader('content-type', 'application/json')
        self.request.write( self.serializer( methodfunction(**parsed_args) ) )
        yield None

    def _parse_args(self, args):
        """
        Universal request arguments parsing and type convertions
        """
        parsed_args = {}
        # parameters parsing
        for key in args.iterkeys():
            if key not in self.argument_types:
                continue
                continue
            # empty args handling
            if args[key][0] == '':
                parsed_args[key] = None
            # boolean args handling
            elif self.argument_types[key] == bool:
                if args[key][0] == 'True': parsed_args[key] = True
                if args[key][0] == 'False': parsed_args[key] = False
            # list args
            elif self.argument_types[key] == list:
                parsed_args[key] = self.argument_types[key](args[key])
            elif self.argument_types[key] == parse_qs:
                parsed_args[key] = self._parse_args( self.argument_types[key](args[key][0]) )
            else:
                parsed_args[key] = self.argument_types[key](args[key][0])
        return parsed_args

    def _unregister(self, passthrough):
        self.request.unregisterProducer()
        return passthrough

    def pauseProducing(self):
        self._task.pause()

    def resumeProducing(self):
        self._task.resume()

    def stopProducing(self):
        self._task.stop()


class TinaServer(resource.Resource):
    """
    Main Server class
    dynamically dispatching URL to a TinaServerResource() class
    """
    def __init__(self, tinacallback, posthandler, gethandler):
        self.callback = tinacallback
        self.posthandler = posthandler
        self.gethandler = gethandler
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        try:
            if request.method == 'POST':
                handler = self.posthandler
                getattr(handler, name)
            elif request.method == 'GET':
                handler = self.gethandler
                getattr(handler, name)
            else:
                raise Exception()
        except:
            return NoResource()
        else:
            return TinaServerResource(handler, name, self.callback, self.posthandler.tinaappinstance.logger)


class TinaAppPOST(object):
    """
    TinaApp mapping POST requests and TinaApp's methods
    """
    def __init__(self, tinaappinstance):
        self.tinaappinstance = tinaappinstance

    def file(self, *args, **kwargs):
        """ index a file given a whitelist into a dataset db"""
        return self.tinaappinstance.index_file(*args, **kwargs)

    def graph(self, *args, **kwargs):
        """ generate a graph given a dataset db, a whitelist and some graph params"""
        return self.tinaappinstance.generate_graph(*args, **kwargs)

    #def dataset(self, corporaobj):
        """ insert """
        #self.tinaappinstance.set_storage( corporaobj['id'] )
        #return self.tinaappinstance.storage.insertCorpora(corporaobj)

    #def corpus(self, dataset, corpusobj):
        """ insert """
        #self.tinaappinstance.set_storage( dataset )
        #return self.tinaappinstance.storage.insertCorpus(corpusobj)

    #def document(self, dataset, documentobj):
        """ insert """
        #self.tinaappinstance.set_storage( dataset )
        #return self.tinaappinstance.storage.insertDocument(documentobj)

    #def ngram(self, dataset, ngramobj):
        """ insert """
        #self.tinaappinstance.set_storage( dataset )
        #return self.tinaappinstance.storage.insertNgram(ngramobj)


class TinaAppGET(object):
    """
    TinaApp mapping GET requests and TinaApp's methods
    """
    def __init__(self, tinaappinstance, stream):
        self.stream = stream
        self.tinaappinstance = tinaappinstance

    def file(self, *args, **kwargs):
        """
        runs an extraction process and exports
        a whitelist csv file for a given dataset and source file
        """
        return self.tinaappinstance.extract_file(*args, **kwargs)

    #def whitelist(self, *args, **kwargs):
        """
        exports a whitelist csv file
        for a given dataset, periods, and whitelist
        """
    #    return self.tinaappinstance.export_whitelist(*args, **kwargs)

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

    def walk_source_files(self):
        """list any existing fily for a given dataset and filetype"""
        return self.tinaappinstance.walk_source_files()

    def open_user_file(self, fileurl):
        browser  = webbrowser.get()
        decoded = urllib.unquote_plus(fileurl)
        return browser.open(decoded.replace("%5C","\\").replace("%2F","/").replace("%3A",":"))

    def exit(self):
        """exit and return nothing"""
        reactor.stop()

    def log(self):
        lines = []
        self.stream.seek(0)
        for line in self.stream:
            lines += [line.strip("\n")]
        self.stream.truncate(0)
        return lines

class NumpyFloatHandler(jsonpickle.handlers.BaseHandler):
    """
    Automatic conversion of numpy float  to python floats
    Required for jsonpickle to work correctly
    """
    def flatten(self, obj, data):
        """
        Converts and round to float an encod
        """
        return round(obj,6)


jsonpickle.handlers.registry.register(numpy.float, NumpyFloatHandler)
jsonpickle.handlers.registry.register(numpy.float32, NumpyFloatHandler)
jsonpickle.handlers.registry.register(numpy.float64, NumpyFloatHandler)


class TinaServerCallback(object):
    """
    Tinaserver's universal callback class
    """
    default = ""

    def serialize(self, obj):
        """
        Encoder to send messages to the host appllication
        """
        return jsonpickle.encode(obj)

    def deserialize(self, serialized):
        """
        Decoder for the host's application messages
        """
        return jsonpickle.encode(serialized)

    def success(self, response):
        """
        writes the success json string
        but still checks STATUS_ERROR in case of caught error during request
        """
        if response == TinaApp.STATUS_ERROR:
            response = traceback.format_exc()
        if response == None:
            response = self.default
        return self.serialize( response )


class LoggerHandler(logging.StreamHandler):
    """
    modified handler logging.StreamHandler
    """
    def __init__(self, *args, **kwargs):
        logging.StreamHandler.__init__(self, *args, **kwargs)
        formatter = logging.Formatter("%(message)s")
        self.setFormatter(formatter)


#class LoggerServer(Protocol):

#    def connectionMade(self):
        """
        read last lines from the logger
        """
#        self.factory.stream.seek(0)
#        for line in self.factory.stream:
#            self.transport.write( line+'\r\n' )
#        self.factory.stream.truncate(0)
#        self.transport.loseConnection()


#class LoggerServerFactory(Factory):

#    protocol = LoggerServer

#    def __init__(self, stream):
#        self.stream = stream


def run(confFile):
    custom_logger = logging.getLogger('TinaAppLogger')
    stream = open(tempfile.mkstemp()[1], mode='w+')
    custom_logger.addHandler(LoggerHandler( stream ))
    # unique tinaapp instance with the custom logger
    tinaappsingleton = TinaApp(confFile, custom_logger=custom_logger)
    # Main server instance
    tinaserver = TinaServer(
        TinaServerCallback(),
        TinaAppPOST(tinaappsingleton),
        TinaAppGET(tinaappsingleton, stream)
    )
    # the user generated files directory is served as-is
    tinaserver.putChild("user", File(tinaappsingleton.user) )
    # optionally serves the "static" website directory
    if exists('static'):
        tinaserver.putChild("", File('static'))

    site = server.Site(tinaserver)
    reactor.listenTCP(8888, site)
    reactor.run()

def usage():
    print "USAGE : python httpserver.py yaml_configuration_file_path"

if __name__ == "__main__":
    import getopt
    opts, args = getopt.getopt(sys.argv[1:],'')
    try:
        confFile=args[0]
    except:
        usage()
        exit()
    run(confFile)
