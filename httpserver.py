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

from twisted.web import server, resource
from twisted.internet import reactor, threads
from twisted.web.static import File
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
        'fileurl': str,
        'ngramgraphconfig': parse_qs,
        'documentgraphconfig': parse_qs,
        'proximity': str,
        'alpha': float,
        'nodethreshold': list,
        'edgethreshold': list,
    }
    def __init__(self, method, back, logger):
        self.method = method
        self.back = back
        self.logger = logger
        resource.Resource.__init__(self)

    def _parse_args(self, args):
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
        # parameters parsing
        parsed_args = self._parse_args(request.args)
        print self.method, parsed_args
        d = defer.Deferred()
        request.setHeader("content-type", "application/json")
        # sends the request through the callback
        try:
            deferred = threads.deferToThread( self.method, None, **parsed_args)
            deferred.addBoth( self.back.call )
        except:
            self.logger.error( traceback.format_exc() )
            return ErrorPage(500, "tinasoft server fatal error, please report it",
                traceback.format_exc() ).render(request)

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
            return TinaServerResource(method, self.callback, self.posthandler.tinaappinstance.logger)

class TinaAppPOST(object):
    """
    TinaApp mapping POST requests on an instance of TinaApp's
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
    TinaApp mapping GET requests on an instance of TinaApp's
    """
    def __init__(self, tinaappinstance, loggerstream):
        self.loggerstream = loggerstream
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
        """exit"""
        reactor.stop()

    def log(self):
        """
        read last lines from the logger
        """
        lines = []
        self.loggerstream.seek(0)
        for line in self.loggerstream:
            lines += [line]
        self.loggerstream.truncate(0)
        return lines


class NumpyFloatHandler(jsonpickle.handlers.BaseHandler):
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
    Tinaserver's callback class
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

    def call(self, returnValue=None):
        #_observerProxy.notifyObservers(None, msg, jsonpickle.encode( returnValue ))
        if returnValue == TinaApp.STATUS_ERROR:
            return ErrorPage(500, "tinasoft server non-fatal error, please report it",
                traceback.format_exc() ).render(request)

        if returnValue == None:
            returnValue = self.default
        return self.serialize( returnValue )

class LoggerHandler(logging.StreamHandler):
    """
    overwrites logging.StreamHandler behaviour
    """
    def __init__(self, *args, **kwargs):
        logging.StreamHandler.__init__(self, *args, **kwargs)
        formatter = logging.Formatter("%(message)s")
        self.setFormatter(formatter)

def run(confFile):
    custom_logger = logging.getLogger('TinaAppLogger')
    stream = open(tmp = tempfile.mkstemp()[1], mode='a+')
    custom_logger.addHandler(LoggerHandler( stream ))

    tinaappsingleton = TinaApp(confFile, custom_logger=custom_logger)

    # specialized GET and POST handlers
    posthandler = TinaAppPOST(tinaappsingleton)
    gethandler = TinaAppGET(tinaappsingleton, stream)
    # Callback class
    callbacks = TinaServerCallback()
    # Main server instance
    tinaserver = TinaServer(callbacks, posthandler, gethandler)
    # generated file directory
    tinaserver.putChild("user", File(tinaappsingleton.user) )
    # static website serving, if static directory exists
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
