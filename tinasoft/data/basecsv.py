# -*- coding: utf-8 -*-
#  Copyright (C) 2010 elishowk
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

from tinasoft.data import Importer, Handler, Exporter

import codecs
import csv
from decimal import *

import logging
_logger = logging.getLogger('TinaAppLogger')

class SafeCsvReader(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, file, encoding, *args, **kwargs):
        self.file = file
        self.encoding = encoding
        self.args = args
        self.kwargs = kwargs

    def decode(self):
        try:
            while 1:
                read = self.file.next()
                try:
                    line = unicode( read, self.encoding, 'ignore' )
                    deccsv = csv.reader( [line], *self.args, **self.kwargs ).next()
                    yield [ unicode( value, self.encoding, 'ignore' ) for value in deccsv]
                except Exception, csverr:
                    _logger.error("in SafeCsvReader: %s"%str(csverr))
                    continue
        except StopIteration, si:
            return

class SafeCsvDictReader():
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, file, encoding, fieldnames, *args, **kwargs):
        self.file = file
        self.fieldnames = fieldnames
        self.encoding = encoding
        self.args = args
        self.kwargs = kwargs

    def decode(self):
        try:
            while 1:
                read = self.file.next()
                try:
                    line = unicode( read, self.encoding, 'ignore' )
                    deccsv = csv.DictReader( [line], self.fieldnames, *self.args, **self.kwargs ).next()
                    for key, value in deccsv.iteritems():
                        deccsv[key] = unicode( value, self.encoding, 'ignore' )
                    yield deccsv
                except Exception, csverr:
                    _logger.error("in SafeCsvDictReader: %s"%str(csverr))
                    continue
        except StopIteration, si:
            return



class Importer (Importer):
    """importer class for a csv file"""
    options = {
        'encoding': 'utf-8',
        'delimiter': ',',
        'quotechar': '"',
    }


    def __init__(self,
            path,
            delimiter=',',
            quotechar='"',
            **kwargs
        ):
        self.path = path
        self.loadOptions( kwargs )
        # CSV format
        self.delimiter = delimiter
        self.quotechar = quotechar
        # gets columns names
        f1 = self.open( self.path )
        if f1 is None:
            return
        tmp = SafeCsvReader(
            f1,
            self.encoding,
            delimiter=self.delimiter,
            quotechar=self.quotechar
        ).decode()
        self.fieldnames = tmp.next()
        del f1, tmp
        # open the file in a Dict mode
        self.file = self.open( self.path )
        self.csv = SafeCsvDictReader(
            self.file,
            self.encoding,
            self.fieldnames,
            delimiter=self.delimiter,
            quotechar=self.quotechar
        ).decode()
        try:
            self.csv.next()
        except Exception, exc:
            _logger.error(str(exc))

    def open( self, path ):
        return open(path, 'rU')

class Exporter (Handler):
    """home-made exporter class for a csv file"""

    def __init__(self,
            filepath,
            delimiter = ',',
            quotechar = '"',
            **kwargs
        ):
        self.loadOptions(kwargs)
        self.filepath = filepath
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.file = codecs.open(self.filepath, "w+", encoding=self.encoding, errors='replace' )

    def writeRow( self, row ):
        """
        writes a csv row to the file handler
        """
        line=[]
        for cell in row:
            if isinstance(cell, str) is True or isinstance(cell, unicode) is True:
                line += ["".join([self.quotechar,cell.replace('"',"'"),self.quotechar])]
            else:
                line += [str(cell)]
        self.file.write( self.delimiter.join(line) + "\n" )

    def writeFile( self, columns, rows ):
        """
        Iterates over a list of rows and calls
        """
        self.writeRow( columns )
        for row in rows:
            self.writeRow( row )
