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

from tinasoft.data import Importer, Handler, Exporter

import codecs
import csv
import cStringIO

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

class UnicodeDictReader(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, fields, dialect='excel', encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.DictReader(f, fields, dialect=dialect, **kwds)

    def next(self):
        try:
            row = self.reader.next()
        except StopIteration, si:
            raise StopIteration(si)
        except Exception, ex:
            _logger.error("basecsv reader error at line %d, reason : %s"%(self.reader.line_num, ex))
            # returns None : child or using object should verify the returning value
            return
        else:
            unicoderow={}
            for k,s in row.iteritems():
                if type(s)==str:
                    unicoderow[k]= unicode(s, "utf-8", errors='replace')
                else:
                    unicoderow[k]=s
            return unicoderow

    def __iter__(self):
        return self

class Importer (Importer,UnicodeDictReader):
    """
    importer class for a csv file using encoding and decoding following
    the example from
    """

    # defaults
    options = {
        'encoding': 'utf-8',
        'dialect': 'excel',
    }

    def __init__(self,
            path,
            **kwargs
        ):
        self.path = path
        self.loadOptions( kwargs )
        # gets columns names
        f1 = self.open( self.path )
        if f1 is None:
            return
        tmp = csv.reader( f1, dialect=self.dialect, quoting=csv.QUOTE_NONNUMERIC )
        self.fieldnames = tmp.next()
        del f1, tmp
        # open the file in a Dict mode
        self.file = self.open( self.path )
        UnicodeDictReader.__init__(
            self,
            self.file,
            self.fieldnames,
            dialect = self.dialect,
            encoding = self.encoding,
            quoting = csv.QUOTE_NONNUMERIC
        )
        try:
            for line in self:
                break
        except Exception, exc:
            _logger.error("error reading first csv line : %s"%(str(exc)))

    def open( self, path ):
        """
        read-only binary file handler
        """
        return open( path, 'rb' )

    def _coerce_unicode(self, cell):
        """
        checks a value and eventually convert to type
        """
        #if type(cell) == int or type(cell) == float:
        #    cell = str(cell)
        if type(cell) != unicode:
            return unicode(cell, "utf-8", errors='replace')
        else:
            return cell


class Exporter (Handler):
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
