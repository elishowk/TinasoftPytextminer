# -*- coding: utf-8 -*-
from tinasoft.data import basecsv

# get tinasoft's logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class Exporter(basecsv.Exporter):
    """A class for csv exports of NGrams Whitelists"""

    def export_cooc(self, storage, periods, whitelist):
        """exports a reconstitued cooc matrix, applying whitelist filtering"""
        for corpusid in periods:
            try:
                generator = storage.selectCorpusCooc( corpusid )
                while 1:
                    ng1, row = generator.next()
                    if whitelist is not None and ng1 not in whitelist:
                        continue
                    for ng2, cooc in row.iteritems():
                        if cooc > row[ng1]:
                            _logger.error( "inconsistency cooc %s %d > %d occur %s" % (ng2, cooc, row[ng1], ng1) )
                        if whitelist is not None and ng2 not in whitelist:
                            continue
                        self.writeRow([ ng1, ng2, str(cooc), corpusid ])
            except StopIteration, si:
                continue
        return self.filepath
