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


__author__="Elias Showk"
__date__ ="$Jul 23, 2010$"

import logging
_logger = logging.getLogger('TinaAppLogger')

# english language default stemmer
from nltk import PorterStemmer


class Nltk():
    """
    Interface to nltk's Stemmers
    """
    def __init__(self, stemmer=None ):
        if stemmer is None:
            stemmer = PorterStemmer()
        self.stemmer = stemmer

    def stem( self, word ):
        """returns the stem of @word"""
        return self.stemmer.stem(word)
