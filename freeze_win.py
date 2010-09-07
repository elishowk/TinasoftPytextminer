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

from os.path import join
from glob import glob
import platform
from cx_Freeze import setup, Executable

data_files = [
#    ('Microsoft.VC90.CRT',glob(join('Microsoft.VC90.CRT','*.*'))),
    'README',
    'LICENSE',
    'config_win.yaml',
    join('shared','gexf','gexf.template'),
#    (join('shared','gexf'),join('shared','gexf','gexf.template')),
#    (join('shared','stopwords'),glob(join('shared','stopwords','*.txt'))),
#    (join('shared','nltk_data','corpora','brown'), join('shared','nltk_data','corpora','brown','*.*')),
#    (join('shared','nltk_data','corpora','conll2000'), glob(join('shared','nltk_data','corpora','conll2000','*.*'))),
#    (join('shared','nltk_data','tokenizers','punkt'), glob(join('shared','nltk_data','tokenizers','punkt','*.*')))
]
data_files += glob(join('Microsoft.VC90.CRT','*.*'))
data_files += glob(join('shared','stopwords','*.txt'))
data_files += glob(join('shared','nltk_data','corpora','brown','*'))
data_files += glob(join('shared','nltk_data','corpora','conll2000','*'))
data_files += glob(join('shared','nltk_data','tokenizers','punkt','*'))

print data_files
# for win32, see: http://wiki.wxpython.org/cx_freeze

includes = [
  'tinasoft.data.basecsv',
  'tinasoft.data.coocmatrix',
  'tinasoft.data.gexf',
  'tinasoft.data.medline',
  #'tinasoft.data.tinabsddb',
  'tinasoft.data.tinasqlite',
  'tinasoft.data.tinacsv',
  'tinasoft.data.whitelist',
  'jsonpickle','tenjin','simplejson'
]
excludes = ['_gtkagg', '_tkagg', 'curses', 'email', 'pywin.debugger',
            'pywin.debugger.dbgcon', 'pywin.dialogs', 'tcl',
            'Tkconstants', 'Tkinter','bsddb3','tinasoft.data.tinabsddb']
packages = ['nltk', 'numpy', 'twisted', 'twisted.web','twisted.internet','encodings','zope.interface']
setup(
        name = "tinasoft",
        version = __version__,
        description = __longdescr__,
        executables = [Executable("httpserver.py")],
        options = {
            "build_exe": {
                "includes": includes,
                "excludes": excludes,
                "packages": packages,
                "include_files": data_files,
            }
        },
)
