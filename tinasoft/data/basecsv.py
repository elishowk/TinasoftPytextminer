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
#  along with this program.  If not, see <http://www.gnu.org/licenses/gpl.html>.

from tinasoft.data import Importer as BaseImporter
from tinasoft.data import Exporter as BaseExporter


import codecs
import csv

import logging
_logger = logging.getLogger('TinaAppLogger')

class UTF8Recoder(object):
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f, 'replace')

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8", 'replace')

class Importer(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, path, **kwargs):
        # gets columns names
        f1 = self.open( path )
        if f1 is None:
            return
        
        tmp = csv.reader( f1, dialect=kwargs['dialect'], quoting=csv.QUOTE_NONNUMERIC )
        self.fieldnames = tmp.next()
        del f1, tmp

        f = UTF8Recoder(self.open(path), kwargs['encoding'])
        self.reader = csv.DictReader(f, self.fieldnames, kwargs['dialect'])
        
        # only reads the first line
        for line in self:
            break
        
    def next(self):
        try:
            row = self.reader.next()
        except StopIteration, si:
            raise StopIteration(si)
        except Exception, ex:
            _logger.error("basecsv reader error at line %d, reason : %s"%(self.reader.line_num, ex))
            return None
        else:
            unicoderow={}
            for k,s in row.iteritems():
                unicoderow[k] = self._coerce_unicode(s)
            return unicoderow

    def __iter__(self):
        return self

    def open(self, path):
        """
        read-only binary file handler
        """
        return open( path, 'rb' )

class Exporter(BaseExporter):
    """
    home-made exporter class for a csv file
    """
    # defaults
    options = {
        'encoding': 'utf-8',
        'delimiter': ',',
        'quotechar': '"',
    }

    def __init__( self, filepath, **kwargs ):
        self.loadOptions(kwargs)
        self.filepath = filepath
        self.file = codecs.open( self.filepath, "w+", encoding=self.encoding, errors='replace' )

    def __del__(self):
        self.file.close()

    def writeRow( self, row ):
        """
        writes a csv row to the file handler
        """
        line=[]
        try:
            for cell in row:
                if isinstance(cell, str) is True or isinstance(cell, unicode) is True:
                    # there should not be " in cells !!
                    line += ["".join([self.quotechar,cell.replace('"',"'"),self.quotechar])]
                elif isinstance(cell, float) is True:
                    #line += ["".join([self.quotechar,"%.2f"%round(cell,2),self.quotechar])]
                    # AVOID OPENOFFICE FLOAT TO DATE AUTO CONVERSION !!!!
                    line += ["%.2f"%round(cell,2)]
                else:
                    line += [str(cell)]

            self.file.write( self.delimiter.join(line) + "\n" )
        except Exception, ex:
            _logger.error("error basecsv.Exporter : %s"%ex)

    def writeFile( self, columns, rows ):
        """
        Iterates over a list of rows and calls
        """
        self.writeRow( columns )
        for row in rows:
            self.writeRow( row )
