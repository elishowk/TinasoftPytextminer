# -*- coding: utf-8 -*-
from tinasoft.data import Exporter, Importer

import codecs
import csv
from datetime import datetime
#corpusID;docID;docAuthor;docTitle;docAbstract;index1;index2

class Exporter (Exporter):

    def __init__(self,
        filepath,
        #corpus,
        delimiter = ',',
        quotechar = '"',
        locale = 'en_US.UTF-8',
        dialect = 'excel'
        ):
        self.filepath = filepath
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.locale = locale
        self.encoding =  self.locale.split('.')[1].lower()
        self.dialect = dialect
                
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

    def csvFile( self, columns, rows ):
        file = codecs.open(self.filepath, "w", encoding=self.encoding, errors='replace' )
        file.write( self.delimiter.join( columns ) + "\n" )
        for row in rows:
            # TODO factorize try in Exporter.export()
            #try:
                file.write( self.delimiter.join( row ) + "\n" )
            #except UnicodeEncodeError, ue:
            #    print "warning exporting a csv line ", row, ue
            #except UnicodeDecodeError, ud:
            #    print "warning exporting a csv line ", row, ud
              
                
class Importer (Importer):

    def __init__(self,
            filepath,
            minSize='1',
            maxSize='4',
            delimiter=',',
            quotechar='"',
            locale='en_US.UTF-8',
            **kwargs 
        ):
        self.filepath = filepath
        self.loadOptions( kwargs )
        # locale management
        self.locale = locale
        self.encoding = locale.split('.')[1].lower()
        # CSV format
        self.delimiter = delimiter
        self.quotechar = quotechar
        # Tokenizer args
        self.minSize = minSize
        self.maxSize = maxSize
        
        f1 = self.open( filepath )
        tmp = csv.reader(
                f1,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
        self.fieldNames = tmp.next()
        del f1
        
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

    def open( self, filepath ):
        return codecs.open( filepath,'rU', errors='replace' )
