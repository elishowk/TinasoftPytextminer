# -*- coding: utf-8 -*-
from tinasoft.data import Exporter, Importer
from datetime import datetime
import codecs

from tinasoft.pytextminer import *

class Model ():
    def __init__(self, lines):
        self.binds = {
            "TI"  : ("title", str, ""),
            "AB"  : ("abstract", str, ""),
            "AU"  : ("author", str, ""),
            "FAU" : ("fullname", str, ""),
            "JT"  : ("pubname", str, ""),
            "DP"  : ("pubdate", datetime, None),
            "STAT": ("stat", str, "MEDLINE"),
            "PMID": ("pmid", str, "0"),
        }
        buff = ""

        for line in lines:
            prefix = line[:4].strip().upper()
            raw =  line[6:]

            if len(buff) > 0 and prefix == "":
                buff = "%s%s" % (buff,raw)

            elif prefix == "AB":
                buff = "%s"%raw
            else:
                # complete the abstract buffer and add it as attribute
                if len(buff) > 0:
                    content = "%s" % buff
                    attribute, type, default = self.binds["AB"]
                    try:
                        cont = type(content)
                        self.__setattr__(attribute, content)
                    except Exception, exc:
                        self.__setattr__(attribute, default)
                        #print exc
                        pass
                    buff = ""

                # add the attribute
                content = "%s" % raw
                try:
                    attribute, type, default = self.binds[prefix]
                    try:
                        cont = type(content)
                        self.__setattr__(attribute, content)
                    except Exception, exc:
                        self.__setattr__(attribute, default)
                        #print exc
                        pass
                except Exception, exc:
                    #print exc
                    pass

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

class Importer (Importer):
    def __init__(self, path, **options):
        self.loadOptions(options)
        self.corpusDict = {}
        #self.locale = self.get_property(options, 'locale', 'en_US.UTF-8')
        #self.lang,self.encoding = self.locale.split('.')
        self.file = codecs.open(path, "rU", self.encoding)

    def parse(self):
        """Read Medline records one by one from the handle.

        The handle is either is a Medline file, a file-like object, or a list
        of lines describing one or more Medline records.

        Typical usage:

            from Bio import Medline
            handle = open("mymedlinefile")
            records = Medline.parse(handle)
            for record in record:
                print record['TI']

        """
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
        finished = False
        while not finished:
            if line[:6]=="      ": # continuation line
                record[key].append(line[6:])
            elif line:
                key = str(line[:4].rstrip())
                if not key in record:
                    record[key] = []
                record[key].append(line[6:])
            try:
                line = handle.next()
            except StopIteration:
                finished = True
            else:
                line = line.rstrip()
                if line:
                    continue
            # Join each list of strings into one string.
            for key in textkeys:
                if key in record:
                    record[key] = " ".join(record[key])
            if record and 'DP' in record:
                corpusid = record['DP']
                newdoc = self.parseDocument( record )
                if newdoc is not None:
                    yield newdoc, corpusid
            record = Record()



    def parseDocument(self, model):
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

        return newdoc

    def parseFile(self):
        for line in self.file.readlines():
            lines = []
            # walks the document's group of lines
            line = line.rstrip()
            # document separator is an empty line
            if line != "":
                lines += [line]
            # parses the lines using Model
            model = Model(lines)
            fields={}
            for key in model.binds.iterkeys():
                value, type, default = model.binds[key]
                fields[key] = value
            if hasattr( model, 'pubdate' ):
                newdoc = self.parseDocument( model, fields, model.pubdate )
                # sends the document and the corpus id
                yield newdoc, corpusNumber

#    def parseDocument(self, model, fields, corpusId ):
#        content = "%s %s"%(model.__getattr__(fields['AB']),\
#            model.__getattr__(fields['TI']) )
#        title = model.__getattr__(fields['TI'])
#        pubdate = model.__getattr__(fields['DP'])
#        docid = model.__getattr__(fields['PMID'])
#        del fields['PMID']
#        del fields['DP']
#        del fields['TI']
#        del fields['AB']
#        docArgs = {}
#        for value in fields.itervalues():
#            docArgs[ value ] =  model.__getattr__( value )
#        # document instance
#        newdoc = document.Document(
#            content,
#            docid,
#            title,
#            datestamp = pubdate,
#            **docArgs
#        )
#        # document's edges

#        return newdoc


