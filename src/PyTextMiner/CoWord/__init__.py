# -*- coding: utf-8 -*-
__author__="Elias Showk"

class SimpleAnalysis():
    """
    A homemade coword analyzer
    """ 
    def cooc( self, documentngrams, ng1, ng2 ):
        if ng1 in documentngrams and ng2 in documentngrams:
            return 1
        else:
            return 0
