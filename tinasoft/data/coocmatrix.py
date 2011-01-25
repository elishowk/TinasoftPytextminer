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

# get tinasoft's logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class Exporter(basecsv.Exporter):
    """A class for space separated file exports of Cooccurrences matrix"""

    def _write_cooc_row(self, ngid, row, corpusid, minCooc=1):
        countcooc = 0
        for ng2, cooc in row.iteritems():
            if cooc >= minCooc:
                self.writeRow([ ngid, ng2, cooc, corpusid ])
                countcooc += 1
        return countcooc
        
    def export_from_storage(self, storage, periods, minCooc=1):
        """exports a reconstitued cooc matrix from storage, applying whitelist filtering"""
        totalcooc = 0
        for corpusid in periods:
            try:
                generator = storage.selectCorpusGraphPreprocess(corpusid, "NGram")
                while 1:
                    ng1, row = generator.next()
                    totalcooc += self._write_cooc_row(ng1, row, corpusid, minCooc)
                    yield totalcooc
            except StopIteration, si:
                _logger.debug("Exported %d non-zeros cooccurrences for period %s"%(totalcooc,corpusid))
                yield self.filepath
                return

    def export_matrix(self, matrix, period, minCooc=1):
        """exports one half of a symmetric cooccurrences matrix"""
        generator = matrix.extract_matrix(minCooc)
        totalcooc = 0
        try:
            while 1:
                ngi, row = generator.next()
                totalcooc +=  self._write_cooc_row(ng1, row, period, minCooc)
        except StopIteration, si:
            _logger.debug("Exported %d non-zeros cooccurrences for the period %s"%(totalcooc, period))
            return self.filepath

