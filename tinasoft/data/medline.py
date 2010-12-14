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

from tinasoft.data import sourcefile
from tinasoft.pytextminer import document, corpus

import logging
_logger = logging.getLogger('TinaAppLogger')

from hashlib import sha256

class Record(dict):
    """A dictionary holding information from a Medline record.
    All data are stored under the mnemonic appearing in the Medline
    file. These mnemonics have the following interpretations:

    Mnemonic  Description
    AB        Abstract
    CI        Copyright Information
    AD        Affiliation
    IRAD      Investigator Affiliation
    AID       Article Identifier
    AU        Author
    FAU       Full Author
    CN        Corporate Author
    DCOM      Date Completed
    DA        Date Created
    LR        Date Last Revised
    DEP       Date of Electronic Publication
    DP        Date of Publication
    EDAT      Entrez Date
    GS        Gene Symbol
    GN        General Note
    GR        Grant Number
    IR        Investigator Name
    FIR       Full Investigator Name
    IS        ISSN
    IP        Issue
    TA        Journal Title Abbreviation
    JT        Journal Title
    LA        Language
    LID       Location Identifier
    MID       Manuscript Identifier
    MHDA      MeSH Date
    MH        MeSH Terms
    JID       NLM Unique ID
    RF        Number of References
    OAB       Other Abstract
    OCI       Other Copyright Information
    OID       Other ID
    OT        Other Term
    OTO       Other Term Owner
    OWN       Owner
    PG        Pagination
    PS        Personal Name as Subject
    FPS       Full Personal Name as Subject
    PL        Place of Publication
    PHST      Publication History Status
    PST       Publication Status
    PT        Publication Type
    PUBM      Publishing Model
    PMC       PubMed Central Identifier
    PMID      PubMed Unique Identifier
    RN        Registry Number/EC Number
    NM        Substance Name
    SI        Secondary Source ID
    SO        Source
    SFM       Space Flight Mission
    STAT      Status
    SB        Subset
    TI        Title
    TT        Transliterated Title
    VI        Volume
    CON       Comment on
    CIN       Comment in
    EIN       Erratum in
    EFR       Erratum for
    CRI       Corrected and Republished in
    CRF       Corrected and Republished from
    PRIN      Partial retraction in
    PROF      Partial retraction of
    RPI       Republished in
    RPF       Republished from
    RIN       Retraction in
    ROF       Retraction of
    UIN       Update in
    UOF       Update of
    SPIN      Summary for patients in
    ORI       Original report in
    """
    def __init__(self):
        # The __init__ function can be removed when we remove the old parser
        self.id = ''
        self.pubmed_id = ''

        self.mesh_headings = []
        self.mesh_tree_numbers = []
        self.mesh_subheadings = []

        self.abstract = ''
        self.comments = []
        self.abstract_author = ''
        self.english_abstract = ''

        self.source = ''
        self.publication_types = []
        self.number_of_references = ''

        self.authors = []
        self.no_author = ''
        self.address = ''

        self.journal_title_code = ''
        self.title_abbreviation = ''
        self.issn = ''
        self.journal_subsets = []
        self.country = ''
        self.languages = []

        self.title = ''
        self.transliterated_title = ''
        self.call_number = ''
        self.issue_part_supplement = ''
        self.volume_issue = ''
        self.publication_date = ''
        self.year = ''
        self.pagination = ''

        self.special_list = ''

        self.substance_name = ''
        self.gene_symbols = []
        self.secondary_source_ids = []
        self.identifications = []
        self.registry_numbers = []

        self.personal_name_as_subjects = []

        self.record_originators = []
        self.entry_date = ''
        self.entry_month = ''
        self.class_update_date = ''
        self.last_revision_date = ''
        self.major_revision_date = ''

        self.undefined = []

class Importer(sourcefile.Importer):
    """
    Medline file importer class
    """

    # defaults
    options = {
        'period_size': 4,
        'encoding': 'ascii'
    }

    def __init__(self, path, **options):
        sourcefile.Importer.__init__(self, path, **options)
        self.reader = self.get_record()

    def get_record(self):
        # These keys point to string values
        textkeys = ("ID", "PMID", "SO", "RF", "NI", "JC", "TA", "IS", "CY", "TT",
                    "CA", "IP", "VI", "DP", "YR", "PG", "LID", "DA", "LR", "OWN",
                    "STAT", "DCOM", "PUBM", "DEP", "PL", "JID", "SB", "PMC",
                    "EDAT", "MHDA", "PST", "AB", "AD", "EA", "TI", "JT")
        handle = iter(self.file)
        self.line_num = 0
         # Skip blank lines at the beginning
        try:
            for line in handle:
                self.line_num+=1;
                line = line.rstrip()
                if line:
                    break
                else:
                    continue
        except Exception, exc:
            _logger.error("medline error reading FIRST lines : %s"%exc)

        record = Record()
        while 1:
            if line[:6]=="      ": # continuation line
                # there's already a key
                if key not in record:
                    record[key] = []
                record[key].append(line[6:])
            elif line:
                key = str(line[:4].rstrip())
                if not key in record:
                    record[key] = []
                record[key].append(line[6:])
            try:
                # next line
                line = handle.next()
                self.line_num+=1;
            except StopIteration, si:
                return
            except Exception, exc:
                _logger.error("medline error reading line %d : %s"%(self.line_num, exc))
                continue
            else:
                # cleans line and jump to next iteration
                line = line.rstrip()
                if line:
                    continue
            # Join each list of strings into one string.
            for key in textkeys:
                if key in record:
                    record[key] = " ".join(record[key])

            record['DP'] = self._parse_period(record)

            yield record
            record = Record()

    def _parse_period(self, record):
        if self.fields['corpus_id'] not in record:
            return None
        return record[ self.fields['corpus_id'] ][ : self.period_size]

    def next(self):
        try:
            row = self.reader.next()
        except StopIteration, si:
            raise StopIteration(si)
        except Exception, ex:
            _logger.error("basecsv reader error at line %d, reason : %s"%(self.reader.line_num, ex))
            # returns None : child or using object should verify the returning value
            return None
        else:
            return row

    def __iter__(self):
        return self