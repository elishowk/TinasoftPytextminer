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

from tinasoft.data import Handler

import codecs
import csv
#from datetime import datetime
import logging
_logger = logging.getLogger('TinaAppLogger')

class Exporter (Handler):
    """exporter class for a csv file"""

    def __init__(self,
            filepath,
            delimiter = ',',
            quotechar = '"',
            #dialect = 'excel',
            **kwargs
        ):
        #Handler.__init__(self)
        self.loadOptions(kwargs)
        self.filepath = filepath
        self.delimiter = delimiter
        self.quotechar = quotechar
        #self.dialect = dialect
        self.file = codecs.open(self.filepath, "w+", encoding=self.encoding, errors='replace' )

    def writeRow( self, row ):
        #row=map(str, row)
        self.file.write( self.delimiter.join( \
            ["".join([self.quotechar,cell,self.quotechar]) \
            for cell in row] ) + "\n" )

    def writeFile( self, columns, rows ):
        self.writeRow( columns )
        for row in rows:
            self.writeRow( row )


class Importer (Handler):
    """importer class for a csv file"""

    def __init__(self,
            filepath,
            delimiter=',',
            quotechar='"',
            **kwargs
        ):

        self.filepath = filepath
        self.loadOptions( kwargs )
        # CSV format
        self.delimiter = delimiter
        self.quotechar = quotechar
        # gets columns names
        f1 = self.open(filepath)
        if f1 is None:
            return
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


    def open( self, filepath ):
        try:
            return codecs.open( filepath,'rU', errors='replace' )
        except IOError, ioerror:
            _logger.error(ioerror)
            return None

