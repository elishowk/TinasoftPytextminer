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
        row = self.reader.next()
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
    """home-made importer class for a csv file"""

    # defaults
    options = {
        'encoding': 'utf-8',
        'dialect': 'excel',
        #'quotechar': '"',
        #'delimiter': ','
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
            #quotechar = self.quotechar,
            #delimiter = self.delimiter,
            encoding = self.encoding,
            quoting=csv.QUOTE_NONNUMERIC
        )
        try:
            for line in self:
                break
        except Exception, exc:
            _logger.error("error reading first csv line : %s"%(str(exc)))

    def open( self, path ):
        #return codecs.open( path, 'rU', encoding=self.encoding, errors='replace' )
        return open( path, 'rb' )

class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect='excel', encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8", 'replace') for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8", 'replace')
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data, 'replace')
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

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
