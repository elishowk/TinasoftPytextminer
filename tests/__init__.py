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

__author__ = "elishowk@nonutc.fr"

#Works with the following CONFIGURATION:
#
#datasets:
#    doc_extraction:
#        # the following values can be document object fields:
#        # - defined by one of the field's value
#        # - constants required fields : 'content'/'label'/'id'
#        - 'content'
#        - 'title'
#    # tina csv columns declaration
#    # will ignore undeclared fields
#    # and warn not found optional fields
#    tinacsv:
#        # doc_label represents one of the field's key
#        # if not found in the file, will use the field specified by "label"
#        doc_label: 'acronym'
#        fields:
#            # required fields
#            label: 'doc_id'
#            content: 'abstract'
#            corpus_id: 'corp_id'
#            id: 'doc_id'
#            # optionnal fields
#            title: 'title'
#            acronym: 'acronym'

def get_tinacsv_test_3_data(ngram_prox="cooccurrences", doc_prox="sharedNGrams"):
    return {
        'nodes' : {
            # matches "TINA" and "tina"
            "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": {
                'weight': 3,
                'label': "TINA EC ICT FP7 CA"
            },
            # matches "visualisation" and "visualization"
            "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98": {
                'weight': 2,
                'label': "visualisation"
            },
            "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": {
                'weight': 2,
                'label': "analysis and visualisation"
            },
            "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": {
                'weight': 2,
                'label': "scientific research"
            },
            "NGram::a9f6eb8ed71f6e1d6e8121cf05e6d7d3362382010d973681a9e2351e3ab2c958": {
                'weight': 1,
                'label': "interactive assessement"
            },
            "Document::60": {
                'weight': 1,
                'label': "DUPLICATE"
            },
            "Document::59": {
                'weight': 1,
                'label': "TINA"
            },
            "Document::55": {
                'weight': 1,
                'label': "UNIQUE"
            },
        },
        'edges': {
            # "TINA" and "tina"
            "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e" : {
                "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98": 2,
                "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": 2,
                "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": 2,
                "NGram::a9f6eb8ed71f6e1d6e8121cf05e6d7d3362382010d973681a9e2351e3ab2c958": 1,
                "Document::60": 1,
                "Document::59": 1,
                "Document::55": 1,
            },
            # "visualisation" and "visualization"
            "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98" : {
                "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": 2,
                "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": 2,
                "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": 2,
                "NGram::a9f6eb8ed71f6e1d6e8121cf05e6d7d3362382010d973681a9e2351e3ab2c958": 1,
                "Document::60": 1,
                "Document::59": 2,
            },
            # "analysis and visualisation"
            "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": {
                "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": 2,
                "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98": 2,
                "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": 2,
                "NGram::a9f6eb8ed71f6e1d6e8121cf05e6d7d3362382010d973681a9e2351e3ab2c958": 1,
                "Document::60": 1,
                "Document::59": 1,
            },
            # "scientific research"
            "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": {
                "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": 2,
                "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98": 2,
                "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": 2,
                "NGram::a9f6eb8ed71f6e1d6e8121cf05e6d7d3362382010d973681a9e2351e3ab2c958": 1,
                "Document::60": 1,
                "Document::59": 1,
            },
            # "interactive assessement"
            "NGram::a9f6eb8ed71f6e1d6e8121cf05e6d7d3362382010d973681a9e2351e3ab2c958": {
                "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": 1,
                "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98": 1,
                "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": 1,
                "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": 1,
                "Document::59": 1,
            },
            "Document::60": {
                "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": 1,
                "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98": 1,
                "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": 1,
                "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": 1,
                # it shares "TINA", "analysis and visualisation" & "scientific research"
                "Document::59": 4,
                # it shares "TINA"
                "Document::55": 1,
            },
            "Document::59": {
                "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": 1,
                "NGram::a7a9ed6c4c233c9fc17c8ff33d4f490df8750503939d726ca856d109a2c1bd98": 2,
                "NGram::818c1d3377c1b53a31b756404dd07fcf642fbb59c30ab27c1f92cb1e746570dc": 1,
                "NGram::0d712987367dce537111041aa572fc3f368c50b99b2ed06271840f15ae88af3e": 1,
                "NGram::a9f6eb8ed71f6e1d6e8121cf05e6d7d3362382010d973681a9e2351e3ab2c958": 1,
                # it shares "TINA", "analysis and visualisation" & "scientific research"
                "Document::60": 4,
                # it shares "TINA"
                "Document::55": 1,
            },
            "Document::55": {
                "NGram::3743759f962f379be74fbd207d5c2e8ed68bb56751763772f7370bc13b76bb7e": 1,
                # they both share "TINA"
                "Document::60": 1,
                "Document::59": 1,
            },
        }      
    }