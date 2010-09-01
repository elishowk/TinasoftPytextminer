# -*- coding: utf-8 -*-
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

from tinasoft.data import basecsv
import codecs

# get tinasoft's logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class Exporter(basecsv.Exporter):
    """A class for space separated file exports of Cooccurrences matrix"""
    def __init__(self,
            filepath,
            delimiter = '   ',
            quotechar = '',
            **kwargs
        ):
        basecsv.Exporter.__init__(
            self,
            filepath,
            delimiter,
            quotechar,
            **kwargs
        )
        self.file.close()
        self.file = codecs.open(self.filepath, "a+", encoding=self.encoding, errors='replace' )

    def export_cooc(self, storage, periods, whitelist):
        """exports a reconstitued cooc matrix from storage, applying whitelist filtering"""
        for corpusid in periods:
            try:
                generator = storage.selectCorpusCooc( corpusid )
                while 1:
                    ng1, row = generator.next()
                    if whitelist is not None and not whitelist.text(ng1):
                        continue
                    for ng2, cooc in row.iteritems():
                        if cooc > row[ng1]:
                            _logger.error( "inconsistency cooc %s %d > %d occur %s" % (ng2, cooc, row[ng1], ng1) )
                        if whitelist is not None and not whitelist.text(ng2):
                            continue
                        self.writeRow([ ng1, ng2, cooc, corpusid ])
            except StopIteration, si:
                continue
        return self.filepath

    def export_matrix(self, matrix, period, minCooc=1):
        """exports a SEMI cooccurrences matrix"""
        #import pdb
        #pdb.set_trace()
        size = len( matrix.reverse.keys() )
        for i in range(size):
            for j in range(size-i):
                ngi = matrix.reverse.keys()[i]
                ngj = matrix.reverse.keys()[j]
                cooc = matrix.get(ngi, ngj)
                if(cooc >= minCooc):
                    self.writeRow([ngi, ngj, cooc, period])
        return self.filepath
