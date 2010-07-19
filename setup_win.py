#!/usr/env/python
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

__version__ = "1.0"
__url__= "http://tinasoft.eu"
__descr__ = "A text-mining python module producing bottom-up thematic field graphs"
__license__ = "GNU GPL v3"
__author__ = "elias showk"
__author_email__ = "elishowk at nonutc dot fr"

from setuptools import find_packages
from distutils.core import setup
import py2exe
import os
from os.path import join
from glob import glob
import numpy
from numpy.core import numeric

#PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
#DEPS = glob(os.path.dirname(os.path.abspath(__file__)) + "/deps/*")

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

setup (
    name = 'TinasoftPytextminer',
	#packages = find_packages(),
    packages = ['tinasoft','tinasoft.data','tinasoft.pytextminer',
	#'tinasoft.data.tinabsddb','tinasoft.data.tinacsv',
	#'tinasoft.data.basecsv','tinasoft.data.gexf','tinasoft.data.medline','tinasoft.data.whitelist',
	'tinasoft.pytextminer.ngram','tinasoft.pytextminer.document',
	'tinasoft.pytextminer.corpus','tinasoft.pytextminer.corpora','tinasoft.pytextminer.whitelist',
	'tinasoft.pytextminer.extractor','tinasoft.pytextminer.filtering','tinasoft.pytextminer.stopwords',
	'tinasoft.pytextminer.tagger','tinasoft.pytextminer.tokenizer','tinasoft.pytextminer.cooccurrences'
	],
	#package_dir = {'tinasoft':'tinasoft'},
	#dependency_links=DEPS,
    data_files = data_files,
	#scripts = ['httpserver.py'],
	#console_scripts = ['httpserver.py'],
	#package_data = {'tinasoft':[join('shared'),join('tinasoft\config.yaml','README','LICENSE']},
	requires = ['numpy','yaml','bsddb3','nltk','jsonpickle','tenjin',
		'twisted','twisted.web','twisted.internet','twisted.web.resource','simplejson'
	],
    # py2exe special args
    #zipfile = None,
    console = ["httpserver.py"],
    options = {
		'py2exe': {
			'packages': ['email','encodings','twisted','tinasoft','tinasoft.data','tinasoft.pytextminer',
				'tinasoft.pytextminer.ngram','tinasoft.pytextminer.document',
				'tinasoft.pytextminer.corpus','tinasoft.pytextminer.corpora','tinasoft.pytextminer.whitelist',
				'tinasoft.pytextminer.extractor','tinasoft.pytextminer.filtering','tinasoft.pytextminer.stopwords',
				'tinasoft.pytextminer.tagger','tinasoft.pytextminer.tokenizer','tinasoft.pytextminer.cooccurrences'
			],
			'unbuffered': True,
            'compressed': True,
			'bundle_files': 1,
			'includes': ['numpy','numpy.*','bsddb3','encodings','encodings.*','nltk','jsonpickle','tenjin','twisted','twisted.web','twisted.internet','twisted.web.resource','simplejson'],
			'excludes' : ['Tkconstants','Tkinter','tcl','pdb','unittest','difflib','doctest'],
		},
		'sdist': {
			'formats': 'zip',
		}
    },
    version = __version__,
    url = __url__,
    description = __descr__,
    license = __license__,
    author = __author__,
    author_email = __author_email__
)
