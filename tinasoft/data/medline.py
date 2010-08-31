# -*- coding: utf-8 -*-
from tinasoft.data import Handler
from tinasoft.pytextminer import document, corpus
import codecs

import logging

_logger = logging.getLogger('TinaAppLogger')


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

class Importer(Handler):
    """
    Medline file importer class
    """

    # defaults
    options = {
        'period_size': 4
    }

    def __init__(self, path, **options):
        self.loadOptions(options)
        self.corpusDict = {}
        #self.locale = self.get_property(options, 'locale', 'en_US.UTF-8')
        #self.lang,self.encoding = self.locale.split('.')
        self.file = codecs.open(path, "rU", errors='replace')

    def parsePeriod(self, record):
        if 'DP' not in record:
            return None
        return str(record['DP'][:self.period_size])

    def get_record(self):
        # These keys point to string values
        textkeys = ("ID", "PMID", "SO", "RF", "NI", "JC", "TA", "IS", "CY", "TT",
                    "CA", "IP", "VI", "DP", "YR", "PG", "LID", "DA", "LR", "OWN",
                    "STAT", "DCOM", "PUBM", "DEP", "PL", "JID", "SB", "PMC",
                    "EDAT", "MHDA", "PST", "AB", "AD", "EA", "TI", "JT")
        handle = iter(self.file)
         # First skip blank lines
        for line in handle:
            line = line.rstrip()
            if line:
                break
            else:
                return
        record = Record()
        while 1:
            if line[:6]=="      ": # continuation line
                # there's already a key
                if key not in record:
                    _logger.warning("continuation line without a key")
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
            except StopIteration:
                return
            else:
                # cleans line and jump to next iteration
                line = line.rstrip()
                if line:
                    continue
            # Join each list of strings into one string.
            for key in textkeys:
                if key in record:
                    record[key] = " ".join(record[key])

            yield record
            record = Record()

    def parseFile(self):
        """Read Medline records one by one from the handle.

        The handle is either is a Medline file, a file-like object, or a list
        of lines describing one or more Medline records.

        """
        recordGenerator = self.get_record()
        try:
            while 1:
                record = recordGenerator.next()
                corpusid = self.parsePeriod(record)
                if corpusid is not None:
                    if corpusid not in self.corpusDict:
                        # creates a new corpus and adds it to the global dict
                        self.corpusDict[ corpusid ] = corpus.Corpus( corpusid )
                    newdoc = self.parseDocument( record, corpusid )
                    if newdoc is not None:
                        yield newdoc, corpusid
        except StopIteration:
            return


    def parseDocument(self, model, corpusid):
        try:
            content = "%s %s"%(model['AB'],model['TI'])
            title = model['TI']
            pubdate = model['DP']
            docid = model['PMID']
            del model['PMID']
            del model['DP']
            del model['TI']
            del model['AB']
        except KeyError, ke:
            #_logger.error( "%s key was not found, skipping document"%ke )
            return None
        # document instance
        newdoc = document.Document(
            content,
            docid,
            title,
            datestamp = pubdate,
            **model
        )
        # document's edges
        #newdoc.addEdge('Corpus', corpusid, 1)
        return newdoc

