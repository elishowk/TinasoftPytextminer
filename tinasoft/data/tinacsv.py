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


from tinasoft.data import sourcefile, basecsv

import logging
_logger = logging.getLogger('TinaAppLogger')

class Importer (sourcefile.Importer, basecsv.Importer):
    """
    importer for the tina csv format
    e.g. :
    from tinasoft.data import Reader
    tinaCsvReader = Reader( "tinacsv://file_to_read.csv", config )
    """
    # defaults
    options = {
        'encoding': 'utf_8',
        'dialect': 'excel',
    }
    
    def __init__(self, path, **kwargs):
        sourcefile.Importer.__init__(self, path, **kwargs)
        basecsv.Importer.__init__(self, path, **kwargs)