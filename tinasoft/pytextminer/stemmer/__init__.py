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

import logging
_logger = logging.getLogger('TinaAppLogger')

# english language default stemmer
import nltk
from nltk import PorterStemmer, WordNetLemmatizer

class Identity(object):
    """
    A do nothing stemmer
    """
    def stem( self, word ):
        """returns the same @word"""
        return word

class Nltk(object):
    """
    Interface to nltk's Stemmers
    """

    def __init__(self, language='english'):
        """
        Instanciate PorterStemmer if @stemmer is None
        """
        self.stemmer = PorterStemmer()

    def stem( self, word ):
        """returns the stem of @word"""
        return self.stemmer.stem(word)
