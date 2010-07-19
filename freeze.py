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

__version__="1.0alpha6"
__url__="http://tinasoft.eu"
__longdescr__="A text-mining python module producing thematic field graphs"
__license__="GNU General Public License"
__keywords__="nlp,textmining,graph"
__author__="elias showk"
__author_email__="elishowk@nonutc.fr"
#__classifiers__="nlp textmining http"

import bsddb3

from cx_Freeze import setup, Executable

# for win32, see: http://wiki.wxpython.org/cx_freeze

data_files = [
    ('Microsoft.VC90.CRT', glob(r'e:\\Microsoft.VC90.CRT\\*.*')),
    ('',glob(r'config_win.yaml')),
    ('',glob(r'LICENSE')),
    (join('shared','gexf'), glob(join('shared','gexf','gexf.template'))),
    (join('shared','stopwords'), glob(join('shared','stopwords','*.txt'))),
    (join('shared','nltk_data','corpora','brown'), glob(join('shared','nltk_data','corpora','brown','*.*'))),
    (join('shared','nltk_data','corpora','conll2000'), glob(join('shared','nltk_data','corpora','conll2000','*.*'))),
    (join('shared','nltk_data','tokenizers','punkt'), glob(join('shared','nltk_data','tokenizers','punkt','*.*')))
]

includes = [
  'tinasoft.data.basecsv', 
  'tinasoft.data.coocmatrix',
  'tinasoft.data.gexf',
  'tinasoft.data.medline',
  'tinasoft.data.tinabsddb',
  'tinasoft.data.tinacsv',
  'tinasoft.data.whitelist',
  'bsddb3','jsonpickle','tenjin','simplejson'

]
excludes = ['_gtkagg', '_tkagg', 'curses', 'email', 'pywin.debugger',
            'pywin.debugger.dbgcon', 'pywin.dialogs', 'tcl',
            'Tkconstants', 'Tkinter']
packages = ['bsddb3', 'nltk', 'numpy', 'twisted', 'twisted.web','twisted.internet','encodings','zope.interface']
path = []
setup(
        name = "tinasoft",
        version = __version__,
        description = __longdescr__,
        executables = [Executable("httpserver.py")],
        options = {"build_exe": {"includes": includes,
                                  "excludes": excludes,
                                  "packages": packages,
                                  "path": path
                                  }
                  },
        data_files = data_files
)

