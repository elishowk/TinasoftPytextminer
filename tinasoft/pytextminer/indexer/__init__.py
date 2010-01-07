# -*- coding: utf-8 -*-
__author__="Elias Showk"

import os, os.path
from whoosh.index import *
from whoosh.fields import *
from whoosh.filedb.filestore import FileStorage


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
            print "No existing index at %s : "%self.indexdir, e
            self.schema = TinaSchema()
            if not os.path.exists(self.indexdir):
                os.mkdir(self.indexdir)
            self.index = self.storage.create_index(self.schema)
        except LockError, le:
            print "index LockError %s : "%self.indexdir, le
            raise LockError(le)

    def search( self, documentId ):
        searcher = self.index.searcher()
        return searcher.find( "id", documentId )

    def documentToDict( self, document ):
        return {
            'label' : document.label,
            'id' : document.id,
            'content' : document.content,
            'date' : document.date,
        }

    def write( self, document, writer ):
        docDict = self.documentToDict( document )
        writer.add_document( **docDict )

    def indexDocs( self, docList, overwrite=False ):
        notIndexedDocs = []
        writer = self.index.writer()
        for document in docList:
            if overwrite is True:
				self.write( document, writer )
            else:
                res = self.search( document.id )
                if len( res ) == 0:
                    docDict = self.documentToDict( document )
                    writer.add_document( **docDict )
                else:
                    notIndexedDocs += [ document ]
        writer.commit()
        return notIndexedDocs
