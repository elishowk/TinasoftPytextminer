#!/usr/bin/python
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

from tinasoft import PytextminerFlowApi, LOG_FILE

from twisted.internet.task import cooperate
from twisted.web import server, resource
from twisted.internet import reactor
from twisted.web.static import File
from twisted.python.failure import Failure

# error handling
from twisted.web.resource import NoResource

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
from os import fsync
from os.path import join, exists
import sys
import exceptions

import logging
import tempfile
import platform

class TinasoftServerRequest(resource.Resource):
    """
    The request handler
    """
    def __init__(self, handler, method, callback, logger):
        """
        to be executed method on an object, its callback and a PytextminerFlowApi logger connector
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
                
        #self.logger.info( str(self.method) + " ---" + str(parsed_args) )
        return parsed_args

    def render(self, request):
        """
        Prepares arguments and call the Cooperative method
        """
        d = CooperativeExecution()._method_wrapper(request, self.callback, self.handler, self.method, self.logger)
        d.addCallback(lambda ignored: request.finish())
        d.addErrback(self._method_failed, request)
        return server.NOT_DONE_YET

    def _method_failed(self, err, request):
        """
        error handler
        """
        self.logger.error( str(err) )
        request.setResponseCode(500)
        request.write(str(err))
        request.finish()


class CooperativeExecution(object):
    argument_types = {
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
        'exportedges': bool,
        'object': jsonpickle.decode,
    }

    def _method_wrapper(self, request, serializer, handler, method, logger):
        self.request = request
        self.serializer = serializer
        self.handler = handler
        self.method = method
        self.logger = logger
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
        processGenerator = methodfunction(**parsed_args)
        try:
            lastValue = None
            while 1:
                lastValue = processGenerator.next()
                yield None
        except StopIteration, si:
            self.request.write( self.serializer.success( lastValue ) )

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
        #if isinstance(passthrough, Failure):
        #    print passthrough
        return passthrough

    def pauseProducing(self):
        self._task.pause()

    def resumeProducing(self):
        self._task.resume()

    def stopProducing(self):
        self._task.stop()


class TinasoftServer(resource.Resource):
    """
    Main Server class
    dynamically dispatching URL to a TinaServerResquest class
    """
    def __init__(self, tinacallback, posthandler, gethandler, deletehandler):
        self.callback = tinacallback
        self.posthandler = posthandler
        self.gethandler = gethandler
        self.deletehandler = deletehandler
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        try:
            if request.method == 'POST':
                handler = self.posthandler
                getattr(handler, name)
            elif request.method == 'GET':
                handler = self.gethandler
                getattr(handler, name)
            elif request.method == 'DELETE':
                handler = self.deletehandler
                getattr(handler, name)
            else:
                raise Exception()
        except:
            return NoResource()
        else:
            return TinasoftServerRequest(handler, name, self.callback, self.posthandler.pytmapi.logger)



def value_to_gen(func):
    """
    wrapper to be used for any method not returning a generator
    transforms a value to a generator
    """
    def wrapper(*args, **kwargs):
        yield func(*args, **kwargs)
    return wrapper


class POSTHandler(object):
    """
    Pytextminer API mapping POST requests and Pytextminer API's methods
    """
    def __init__(self, pytmapi):
        self.pytmapi = pytmapi

    def file(self, *args, **kwargs):
        """ index a file given a whitelist into a dataset db"""
        return self.pytmapi.index_file(*args, **kwargs)

    def graph(self, *args, **kwargs):
        """ generate a graph given a dataset db, a whitelist and some graph params"""
        return self.pytmapi.generate_graph(*args, **kwargs)

    @value_to_gen
    def dataset(self, object):
        """ insert or overwrite """
        storage = self.pytmapi.get_storage( corporaobj['id'], create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.insertCorpora(object)

    @value_to_gen
    def corpus(self, dataset, object):
        """ insert or overwrite """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.insertCorpus(object)

    @value_to_gen
    def document(self, dataset, object):
        """ insert or overwrite """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.insertDocument(object)

    @value_to_gen
    def ngram(self, dataset, object):
        """ insert or overwrite """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.insertNGram(object)

class PUTHandler(object):
    """
    Pytextminer API mapping POST requests and Pytextminer API's methods
    """
    def __init__(self, pytmapi):
        self.pytmapi = pytmapi

    @value_to_gen
    def dataset(self, object, recursive):
        """ update """
        storage = self.pytmapi.get_storage( corporaobj['id'], create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.updateCorpora(object, recursive)

    @value_to_gen
    def corpus(self, dataset, object, recursive):
        """ update """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.updateCorpus(object)

    @value_to_gen
    def document(self, dataset, object, recursive):
        """ update """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.updateDocument(object)

    @value_to_gen
    def ngram(self, dataset, object, recursive):
        """ update """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            return self.pytmapi.STATUS_ERROR
        return storage.updateNGram(object)

class GETHandler(object):
    """
    Pytextminer API mapping GET requests and Pytextminer API's methods
    """
    def __init__(self, pytmapi, logstream):
        self.logstream = logstream
        self.pytmapi = pytmapi

    def file(self, *args, **kwargs):
        """
        runs an extraction process and exports
        a whitelist csv file for a given dataset and source file
        """
        return self.pytmapi.extract_file(*args, **kwargs)

    @value_to_gen
    def cooccurrences(self, *args, **kwargs):
        """exports a cooc matrix for a given datasset, periods, and whitelist"""
        return self.pytmapi.export_cooc(*args, **kwargs)

    @value_to_gen
    def graph(self, dataset):
        """list the existing graphs for a given dataset"""
        return self.pytmapi.walk_user_path(dataset, 'gexf')

    @value_to_gen
    def dataset(self, dataset):
        """
        returns a dataset json object from the database
        if argument is empty, returns the list of all datasets
        """
        if dataset is None:
            return self.pytmapi.walk_datasets()
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            self.pytmapi.logger.error("unable to connect to the database")
            return None
        else:
            return storage.loadCorpora(dataset)

    @value_to_gen
    def corpus(self, dataset, id):
        """
        returns a corpus json object from the database
        """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            self.pytmapi.logger.error("unable to connect to the database")
            return None
        else:
            return storage.loadCorpus(id)

    @value_to_gen
    def document(self, dataset, id):
        """
        returns a document json object from the database
        """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            self.pytmapi.logger.error("unable to connect to the database")
            return None
        else:
            return storage.loadDocument(id)

    @value_to_gen
    def ngram(self, dataset, id):
        """
        returns a ngram json object from the database
        """
        storage = self.pytmapi.get_storage( dataset, create=False )
        if storage == self.pytmapi.STATUS_ERROR:
            self.pytmapi.logger.error("unable to connect to the database")
            return None
        else:
            return storage.loadNGram(id)

    @value_to_gen
    def walk_user_path(self, dataset, filetype):
        """lists any existing file for a given dataset and filetype"""
        return self.pytmapi.walk_user_path(dataset, filetype)

    @value_to_gen
    def walk_source_files(self):
        """lists any existing file for a given dataset and filetype"""
        return self.pytmapi.walk_source_files()

    @value_to_gen
    def open_user_file(self, fileurl):
        """commands the OS browser to open a "file://" URL"""
        browser  = webbrowser.get()
        decoded = urllib.unquote_plus(fileurl)
        return browser.open(decoded.replace("%5C","\\").replace("%2F","/").replace("%3A",":"))

    @value_to_gen
    def exit(self):
        """exits and breaks connections"""
        reactor.stop()

    @value_to_gen
    def log(self):
        """logging request sending all lines from a file object then truncating it"""
        lines = []
        self.logstream.seek(0)
        for line in self.logstream:
            lines += [line.strip("\n")]
        self.logstream.truncate(0)
        self.logstream.seek(0)
        self.logstream.flush()
        fsync(self.logstream.fileno())
        return lines

class DELETEHandler(object):
    """
    Pytextminer API mapping DELETE requests and Pytextminer API's methods
    """
    def __init__(self, pytmapi):
        self.pytmapi = pytmapi

    @value_to_gen
    def dataset(self, dataset):
        """ deletes all the dataset's file """
        return self.pytmapi.delete_dataset(dataset)

class NumpyFloatHandler(jsonpickle.handlers.BaseHandler):
    """
    Automatic conversion of numpy float  to python floats
    Required for jsonpickle to work correctly
    """
    def flatten(self, obj, data):
        """
        Converts and rounds a Numpy.float* to Python float
        """
        return round(obj,6)


jsonpickle.handlers.registry.register(numpy.float, NumpyFloatHandler)
jsonpickle.handlers.registry.register(numpy.float32, NumpyFloatHandler)
jsonpickle.handlers.registry.register(numpy.float64, NumpyFloatHandler)


class Serializer(object):
    """
    TinasoftServer's universal callback class
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
        return jsonpickle.decode(serialized)

    def success(self, response):
        """
        writes the success json string
        but still checks STATUS_ERROR in case of caught error during request
        """
        #if response == PytextminerFlowApi.STATUS_ERROR:
        #    response = traceback.format_exc()
        if response == None:
            response = self.default
        return self.serialize( response )


class LoggerHandler(logging.StreamHandler):
    """
    TinasoftServer modified handler logging.StreamHandler
    """
    def __init__(self, *args, **kwargs):
        """
        Overwrites StreamHandler with a formatting
        """
        logging.StreamHandler.__init__(self, *args, **kwargs)
        formatter = logging.Formatter("%(levelname)s - %(message)s")
        self.setFormatter(formatter)


def run(confFile):
    custom_logger = logging.getLogger('TinaAppLogger')
    stream = open(LOG_FILE, mode='w+')
    custom_logger.addHandler(LoggerHandler( stream ))
    # MS windows trick to print to a file server's output to a file
    if platform.system() == 'Windows':
        sys.stdout = stream
        sys.stderr = stream
    # unique PytextminerFlowApi instance with the custom logger
    pytmapi = PytextminerFlowApi(confFile, custom_logger=custom_logger)
    # Main server instance
    pytmserver = TinasoftServer(
        Serializer(),
        POSTHandler(pytmapi),
        GETHandler(pytmapi, stream),
        DELETEHandler(pytmapi)
    )
    # the user generated files directory is served as-is
    pytmserver.putChild("user", File(pytmapi.user) )
    # optionally serves the "static" website directory
    if exists('static'):
        pytmserver.putChild("", File('static'))

    reactor.listenTCP(8888, server.Site(pytmserver))
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
