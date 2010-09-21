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

    def export_cooc(self, storage, periods, whitelist=None, minCooc=1):
        """exports a reconstitued cooc matrix from storage, applying whitelist filtering"""
        countcooc=0
        for corpusid in periods:
            try:
                generator = storage.selectCorpusCooc( corpusid )
                while 1:
                    ng1, row = generator.next()
                    if whitelist is not None and not whitelist.test(ng1):
                        continue
                    for ng2, cooc in row.iteritems():
                        if cooc < minCooc:
                            continue
                        if cooc > row[ng1]:
                            _logger.error( "inconsistency cooc %s %d > %d occur %s" % (ng2, cooc, row[ng1], ng1) )
                        if whitelist is not None and not whitelist.test(ng2):
                            continue
                        countcooc += 1
                        self.writeRow([ ng1, ng2, cooc, corpusid ])
            except StopIteration, si:
                continue
        _logger.debug("Exported %d non-zeros cooccurrences"%countcooc)
        return self.filepath

    def export_matrix(self, matrix, period, minCooc=1):
        """exports one half of a symmetric cooccurrences matrix"""
        generator = matrix.extract_matrix(minCooc)
        countcooc = 0
        try:
            while 1:
                ngi, row = generator.next()
                for ngj,cooc in row.iteritems():
                    countcooc += 1
                    self.writeRow([ngi, ngj, cooc, period])
        except StopIteration, si:
            _logger.debug("Exported %d non-zeros cooccurrences for the period %s"%(countcooc, period))
            return self.filepath


class MatrixStorage():
    """
    OBSOLETE
    Numpy Matrix to Tinasoft storage handler
    """

    def write(self, storage, corpusid, matrix, table, overwrite=True):
        """
        writes in the db rows of the matrix
        'corpus::ngramid' => '{ 'ngx' : y, 'ngy': z }'
        """
        key = corpusid+'::'
        count = 0
        for obji in matrix.reverse.iterkeys():
            row = {}
            for objj in matrix.reverse.iterkeys():
                value = matrix.get( obji, objj )
                if value > 0:
                    count += 1
                    row[objj] = value
            if len( row.keys() ) > 0:
                storage.updateMatrix( name, key+obji, row, overwrite )
        _logger.debug( 'will store %d non-zero cooc values'%count )
        storage.flushQueues()
        _logger.debug( 'finished storing %d non-zero cooc values'%count )

    def read(self, storage ):
        try:
            generator = storage.selectCorpusCooc( self.corpusid )
            while 1:
                id,row = generator.next()
                self.reducer( [id, row] )
        except StopIteration, si:
            return self.matrix
