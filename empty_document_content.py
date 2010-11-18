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

__author__="elishowk@nonutc.fr"

import sys
from tinasoft import PytextminerApi

def usage():
    print "USAGE : python apitests.py TestClass configuration_file_path source_filename file_format"

if __name__ == '__main__':
    print sys.argv
    try:
        confFile = sys.argv[1]
        databaseName = sys.argv[2]
        tinasoft = PytextminerApi(confFile)
    except:
        usage()
        exit()
    tinasoft.set_storage( databaseName )
    documents = tinasoft.storage.loadMany( "Document" )
    try:
        while 1:
            id, doc = documents.next()
            if doc['content'] != "":
                doc['content'] = ""
                print "emptying document %s contents"%doc['id']
                tinasoft.storage.insertDocument(doc, overwrite=True)
            else:
                print "no content in document %s"%doc['id']
    except StopIteration, si:
        exit()
