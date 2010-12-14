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

from tinasoft.data import medline, Handler
from os.path import join

# get tinasoft's logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class Importer(Handler):
    """
    Medline archive handler
    """
    def __init__(self, path, **config):
        if 'medlinearchive' in config:
            self.loadOptions(config['medlinearchive'])
        self.fileconfig = config['medline']
        self.path = path

    def walkArchive(self, periods):
        """
        For a given list of periods
        Yields a medline file reader for each period
        """
        for id in periods:
            abstractFilePath = join(self.path, id, id + '.txt')
            reader = medline.Importer( abstractFilePath, **self.fileconfig )
            reader_gen = reader.parse_file()
            yield reader_gen, id
