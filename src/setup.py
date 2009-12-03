#!/usr/bin/env python
#
# Distutils setup script for PyTextMiner
#
# Copyright (C) 2009-2011 TINA Project CNR
# Authors: elias showk <elishowk@nonutc.fr>
#         julian bilcke <julian.bilcke@iscpif.fr>
# URL: <http://github.com/elishowk/PyTextMiner>
# For license information, see LICENSE.TXT

from distutils.core import setup
import PyTextMiner

setup(
    #############################################
    ## Distribution Metadata
    name = 'PyTextMiner',
    description = "Python text-mining module for co-word analysis",
    
    #version = PyTextMiner.__version__,
    #url = PyTextMiner.__url__,
    #long_description = PyTextMiner.__longdescr__,
    #license = PyTextMiner.__license__,
    #keywords = PyTextMiner.__keywords__,
    #maintainer = PyTextMiner.__maintainer__,
    #maintainer_email = PyTextMiner.__maintainer_email__,
    #author = PyTextMiner.__author__,
    #author_email = PyTextMiner.__author__,
    #classifiers = PyTextMiner.__classifiers__,
    # platforms = <platforms>,
    
    #############################################
    ## Package Data
    #package_data = {'nltk': ['nltk.jar', 'test/*.doctest']},
    #############################################
    ## Package List
    packages = [
                #'numpy',
                #'nltk',
                #'simplejson',
                #'pysqlite',
                #'jsonpickle',
                'PyTextMiner',
                ],
    package_dir = {
        #'numpy': 'numpy/numpy',
        #'nltk': 'nltk/nltk',
        #'simplejson': 'simplejson/simplejson',
        #'pysqlite': 'pysqlite/lib',
        #'jsonpickle': 'jsonpickle/jsonpickle',
        'PyTextMiner': 'PyTextMiner',
    },

    # package dependencies :
    requires = [ 'csv', 'codecs', 'nltk', 'simplejson', 'jsonpickle', 'pysqlite', 'numpy' ],
    )


