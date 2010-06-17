# -*- coding: utf-8 -*-
__author__="Elias Showk"

import os, os.path
from whoosh.index import *
from whoosh.fields import *
from whoosh.filedb.filestore import FileStorage
from whoosh.query import *

import logging
_logger = logging.getLogger('TinaAppLogger')


class TinaSchema(Schema):
    def __init__(self):
        Schema.__init__( self,
            label=TEXT(stored=True),
            id=ID(unique=True, stored=True),
            content=TEXT(stored=True),
            date=TEXT(stored=True),
            )

class TinaIndex():
    """
    Open or Create a whoosh index
    Provides searching methods
    """

    def __init__( self, indexdir ):
        self.writer = None
        self.reader = None
        self.searcher = None
        self.indexdir = indexdir
        self.storage = FileStorage(self.indexdir)
        self.index = None
        try:
            self.index = self.storage.open_index()
        except EmptyIndexError, e:
            _logger.warning( "No existing index at %s : "%self.indexdir)
            self.schema = TinaSchema()
            if not os.path.exists(self.indexdir):
                os.mkdir(self.indexdir)
            self.index = self.storage.create_index(self.schema)
        except LockError, le:
            _logger.error("index LockError %s : "%self.indexdir)
            raise LockError(le)

    def getSearcher( self ):
        return self.index.searcher()

    def getWriter( self ):
        return self.index.writer()

    def searchDoc( self, documentId, field='id' ):
        searcher = self.getSearcher()
        return searcher.find( field, documentId )

    def searchCooc( self, ngrams, field='content' ):
        searcher = self.getSearcher()
        coocReq = And( [Phrase(field, ngram) for ngram in ngrams] )
        return searcher.search(coocReq)

    def countCooc( self, ngrams, field='content' ):
        return len(self.searchCooc( ngrams, field ))

    def objectToDict( self, document ):
        return {
            'label' : document.label,
            'id' : document.id,
            'content' : document.content,
            'date' : document.date,
        }

    def write( self, document, writer, overwrite=False ):
        if overwrite is True:
            docDict = self.objectToDict( document )
            writer.add_document( **docDict )
        else:
            res = self.searchDoc( document['id'], 'id' )
            if len( res ) == 0:
                docDict = self.objectToDict( document )
                writer.add_document( **docDict )
            else:
                return document

    def indexDocs( self, docList, overwrite=False ):
        notIndexedDocs = []
        writer = self.index.writer()
        for document in docList:
            res = self.write(document, writer, overwrite)
            if res is not None:
                notIndexedDocs+=[res]
        writer.commit()
        return notIndexedDocs
