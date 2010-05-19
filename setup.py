#!/usr/bin/env python
#
# Setuptools setup script for tinasoft.PyTextMiner
#
# Copyright (C) 2009-2011 TINA Project FET (euro commision INFSO) / CNRS UMR 7656 CREA (fr)
# Authors: elias showk <elishowk@nonutc.fr>
#         julian bilcke <julian.bilcke@iscpif.fr>
# URL: <http://github.com/elishowk/PyTextMiner>
# For license information, see LICENSE.TXT

from setuptools import setup,find_packages

setup(
    #############################################
    ## Distribution Metadata
    name = 'tinasoft',
    version = '1.0alpha',
    packages = find_packages(),
    namespace_packages = ['tinasoft'],

    # Declare your packages' dependencies here, for eg:
    install_requires=['numpy','jsonpickle','tenjin','whoosh'],

    # Fill in these to make your Egg ready for upload to
    # PyPI
    author = 'elias showk & julian bilcke',
    author_email = 'elishowk@nonutc.fr',

    summary = "Tinasoft's Text-Mining module",
    url = 'http://github.com/elishowk/TinasoftPyTextMiner',
    license = 'GNU GPL v3',

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
    #ext_modules=[Extension('foo', ['foo.c'])],

)
