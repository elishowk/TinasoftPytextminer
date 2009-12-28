#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

from tinasoft.pytextminer import *

class Program( app.TinaExtract ):
    def __init__(self):
        app.TinaExtract.__init__(self)

if __name__ == '__main__':
    program = Program()
    program.main()
