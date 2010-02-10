# -*- coding: utf-8 -*-
from tinasoft.data import Exporter, Importer

import codecs
import csv
from datetime import datetime
import logging
_logger = logging.getLogger('TinaAppLogger')
#corpusID;docID;docAuthor;docTitle;docAbstract;index1;index2

class Exporter (Exporter):

    def __init__(self,
        filepath,
        #corpus,
        delimiter = ',',
        quotechar = '"',
        dialect = 'excel'
        ):
        self.filepath = filepath
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.dialect = dialect
        self.file = codecs.open(self.filepath, "w", encoding=self.encoding, errors='replace' )


    # deprecated
    def objectToCsv( self, objlist, columns ):
        file = codecs.open(self.filepath, "w", self.encoding, errors='replace' )
        file.write( self.delimiter.join( columns ) + "\n" )
        def mapping( att ) :
            print type(att)
            print att
            s = str(att)
            return s
        for obj in objlist:
            attributes = [getattr(obj, col) for col in columns]
            file.write( self.delimiter.join( map( mapping, attributes ) ) + "\n" )
            #file.write( self.delimiter.join( map( self.encode, map( str, attributes ) ) ) + "\n" )

    def writeRow( self, row ):
        self.file.write( self.delimiter.join( row ) + "\n" )

    def writeFile( self, columns, rows ):
        self.writeRow( columns )
        for row in rows:
            #try:
                self.file.write( row )
            #except UnicodeEncodeError, ue:
            #    print "warning exporting a csv line ", row, ue
            #except UnicodeDecodeError, ud:
            #    print "warning exporting a csv line ", row, ud


class Importer (Importer):

    def __init__(self,
            filepath,
            #minSize='1',
            #maxSize='4',
            delimiter=',',
            quotechar='"',
            **kwargs
        ):
        _logger.debug("start of basecsv.Importer.__init__ " + filepath )
        try:
            self.filepath = filepath
            self.loadOptions( kwargs )
            # CSV format
            self.delimiter = delimiter
            self.quotechar = quotechar
            # Tokenizer args
            #self.minSize = minSize
            #self.maxSize = maxSize
            # gets columns names
            f1 = self.open( filepath )
            tmp = csv.reader(
                f1,
                delimiter=self.delimiter,
                quotechar=self.quotechar
            )
            self.fieldNames = tmp.next()
            del f1
            # open the file in a Dict mode
            f2 = self.open( filepath )
            self.csv = csv.DictReader(
                    f2,
                    self.fieldNames,
                    delimiter=self.delimiter,
                    quotechar=self.quotechar)
            self.csv.next()
            del f2
            self.docDict = {}
            self.corpusDict = {}
        except Exception, e:
            _logger.error("error during basecsv.Importer.__init__", + e)
        _logger.debug("end of basecsv.Importer.__init__ " )

    def open( self, filepath ):
        return codecs.open( filepath,'rU', errors='replace' )
