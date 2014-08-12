import sys
from time import time
from invenio.search_engine import get_record, \
    perform_request_search, search_unit_in_bibxxx

from invenio.bibauthorid_logutils import Logger

from invenio.dbquery import run_sql
from invenio.bibauthorid_dbinterface import get_existing_authors, \
    get_canonical_name_of_author, get_papers_of_author, get_all_paper_data_of_author, \
    get_signatures_of_paper, get_author_info_of_confirmed_paper, \
    _get_external_ids_from_papers_of_author, get_claimed_papers_of_author, \
    get_inspire_id_of_signature, populate_partial_marc_caches, move_signature, \
    add_external_id_to_author, get_free_author_id, get_inspire_id_of_author, \
    get_orcid_id_of_author, destroy_partial_marc_caches
from invenio.bibauthorid_general_utils import memoized
import invenio.bibauthorid_dbinterface as db
import invenio.bibauthorid_config as bconfig
from invenio.bibauthorid_webapi import get_hepnames, add_cname_to_hepname_record
from invenio.bibcatalog import BIBCATALOG_SYSTEM

logger = Logger('Hoover')

try:
    from collections import defaultdict
except ImportError:
    from invenio.bibauthorid_general_utils import defaultdict


def open_rt_ticket(e):
    subject = e.get_message_subject()
    body = e.get_message_body()
    debug = e.__repr__() + '\n' + '\n'.join([ str(key) + " " + str(value)  for key, value in vars(e).iteritems() ])
    queue = ''
    if bconfig.HOOVER_OPEN_RT_TICKETS:
        BIBCATALOG_SYSTEM.ticket_submit(uid=None, subject=subject, recordid=-1, text=body+'\n Debugging information: \n'+debug,
                                        queue=queue, priority="", owner="", requestor="")
    else:
        logger.log('sub: '+subject+'\nbody:\n'+body+'\ndbg:\n'+debug)


def timed(func):
    def print_time(*args, **kwds):
        t0 = time()
        res = func(*args, **kwds)
        logger.log(func.__name__, ': with args: ', args, kwds, ' took: ', time() - t0)
        return res
    return print_time


class HooverException(Exception):

    def __init__(self):
        pass

    def get_message_body(self):
        raise NotImplementedError(self.__repr__())

    def get_message_subject(self):
        raise NotImplementedError(self.__repr__())

class ConflictingIdsException(HooverException):

    """Base class for conflicting ids in authors"""

    def __init__(self, message, pid, identifier_type, ids_list):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        pid -- the pid of the author that caused the exception
        identifier -- the type of the identifier that caused the exception
        """
        Exception.__init__(self, message)
        self.pid = pid
        self.identifier_type = identifier_type
        self.ids_list = ids_list


class ConflictingIdsFromReliableSourceException(ConflictingIdsException):

    """Class for conflicting ids in authors that are caused from reliable sources"""
    def get_message_subject(self):
        return "[Hoover] Conflicting identifiers in user verified data"

    def get_massega_body(self):
        msg = ["Found conflicting %s identifiers (%s) on profile: " % (self.identifier_type, ','.join(self.ids_list))]
        msg.append("http://inspirehep.net/author/profile/%s" % get_canonical_name_of_author(self.pid) )
        return '\n'.join(msg)


class ConflictingIdsFromUnreliableSourceException(ConflictingIdsException):

    """Class for conflicting ids in authors that are caused from unreliable sources"""
    def get_message_subject(self):
        return "[Hoover] Conflicting identifiers unverified data"

    def get_message_body(self):
        msg = ["Found conflicting %s identifiers (%s) on profile: " % (self.identifier_type, ','.join(self.ids_list))]
        msg.append("http://inspirehep.net/author/profile/%s" % get_canonical_name_of_author(self.pid) )
        return '\n'.join(msg)


class DuplicatePaperException(HooverException):

    """Base class for duplicated papers conflicts"""

    def __init__(self, message, pid, signature, present_signature):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        pid -- the pid of the author that caused the exception
        signature -- the signature that raise the exception
        """
        Exception.__init__(self, message)
        self.pid = pid
        self.signature = signature
        self.present_signature = present_signature

class DuplicateClaimedPaperException(DuplicatePaperException):

    """Class for duplicated papers conflicts when one of them is claimed"""
    def get_message_subject(self):
        return '[Hoover] Wrong signature claimed to profile'

    def get_message_body(self):
        msg = ['Found wrong signature claimed to profile ']
        msg.append("http://inspirehep.net/author/profile/%s" % get_canonical_name_of_author(self.pid))
        #TODO: add to exception information about which ID is requiring the move
        #TODO: represent signatures in a cataloguer-friendly way
        msg.append("want to move %s to this profile but %s il already present and claimed" %
                    (self.signature, self.present_signature))
        return '\n'.join(msg)

class DuplicateUnclaimedPaperException(DuplicatePaperException):

    """Class for duplicated papers conflicts when one of them is unclaimed"""
    pass


class BrokenHepNamesRecordException(HooverException):

    """Base class for broken HepNames records"""

    def __init__(self, message, recid, identifier_type):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        recid -- the recid of the record that caused the exception
        identifier -- the type of the identifier that caused the exception
        """
        Exception.__init__(self, message)
        self.recid = recid
        self.identifier_type = identifier_type

    def get_message_subject(self):
        return '[Hoover] Found a broken HepNames record'

    def get_message_body(self):
        msg = ['Found broken hepnames record http://inspirehep.net/record/%s' % self.recid]
        msg.append('Something went wroing while trying to read the %s identifier' % self.identifier_type)
        return '\n'.join(msg)

class MultipleHepnamesRecordsWithSameIdException(HooverException):

    """Base class for conflicting HepNames records"""

    def __init__(self, message, recids, identifier_type):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        recids -- an iterable with the record ids that are conflicting
        identifier -- the type of the identifier that caused the exception
        """
        Exception.__init__(self, message)
        self.recids = tuple(recids)
        self.identifier_type = identifier_type

    def get_message_subject(self):
        return '[Hoover] Found conflicting hepnames records'

    def get_message_body(self):
        msg = ['Found conflicting hepnames records: ']
        msg += ['http://inspirehep.net/record/%s' % r for r in self.recids]
        msg.append('Those records are sharing the same %s identifier!' % self.identifier_type)
        return '\n'.join(msg)

class MultipleAuthorsWithSameIdException(HooverException):

    """Base class for multiple authors with the same id"""

    def __init__(self, message, pids, identifier_type):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        pids -- an iterable with the pids that have the same id
        identifier -- the type of the identifier that caused the exception
        """
        Exception.__init__(self, message)
        self.pids = tuple(pids)
        self.identifier_type = identifier_type

    def get_message_subject(self):
        return '[Hoover] Found conflicting profile user-verified identifiers'

    def get_message_body(self):
        msg = ['Found conflicting profiles with conflicting user-verified identifiers: ']
        msg += ['http://inspirehep.net/author/profile/%s' % r for r in self.pids]
        msg.append('Those records are sharing the same %s identifier!' % self.identifier_type)
        return '\n'.join(msg)

class MultipleIdsOnSingleAuthorException(HooverException):

    """Base class for multiple ids on a single author"""

    def __init__(self, message, pid, ids, identifier_type):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        pid -- the pid of the author
        ids -- an iterable with the identifiers of the author
        identifier -- the type of the identifier that caused the exception
        """
        Exception.__init__(self, message)
        self.pid = pid
        self.ids = tuple(ids)
        self.identifier_type = identifier_type

    def get_message_subject(self):
        return '[Hoover] Found profile with multipre conflicting user-verified identifiers'

    def get_message_body(self):
        msg = ['Found profile with multipre conflicting user-verified identifiers: ']
        msg += ['http://inspirehep.net/author/profile/%s' % self.pid]
        msg.append('This profile has all this %s identifiers:' % self.identifier_type)
        msg.append(', '.join(str(x) for x in self.ids))
        msg.append('Each profile should have only one identifier of each kind.')
        return '\n'.join(msg)

class NoCanonicalNameException(HooverException):
    #TODO: not used?

    """Base class for no canonical name found for a pid"""

    def __init__(self, message, pid):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        pid -- the pid of the author that lacks a canonical name
        """
        Exception.__init__(self, message)
        self.pid = pid


def get_signatures_with_inspireID_sql(inspireID):
    """Signatures of specific inspireID using an Sql query"""
    signatures = run_sql("SELECT 100, secondbib.id, firstbibrec.id_bibrec \
                            FROM bib10x AS firstbib \
                            INNER JOIN bibrec_bib10x AS firstbibrec ON firstbib.id=firstbibrec.id_bibxxx \
                            INNER JOIN bibrec_bib10x AS secondbibrec ON firstbibrec.field_number=secondbibrec.field_number \
                                                        AND secondbibrec.id_bibrec=firstbibrec.id_bibrec \
                            INNER JOIN bib10x AS secondbib ON secondbibrec.id_bibxxx=secondbib.id \
                            WHERE firstbib.value=%s AND secondbib.tag='100__a'", (inspireID,)) + \
        run_sql("SELECT 700, secondbib.id, firstbibrec.id_bibrec \
                            FROM bib70x AS firstbib \
                            INNER JOIN bibrec_bib70x AS firstbibrec ON firstbib.id=firstbibrec.id_bibxxx \
                            INNER JOIN bibrec_bib70x AS secondbibrec ON firstbibrec.field_number=secondbibrec.field_number \
                                                        AND secondbibrec.id_bibrec=firstbibrec.id_bibrec \
                            INNER JOIN bib70x AS secondbib ON secondbibrec.id_bibxxx=secondbib.id \
                            WHERE firstbib.value=%s AND secondbib.tag='700__a'", (inspireID,))

    return signatures


def _get_signatures_with_tag_value_cache(value, tag_ending):
    """Signatures of specific inspireID using CACHE"""
    signatures = []
    LC = db.MARC_100_700_CACHE
    if ('100' + tag_ending, value) in LC['inverted_b100']:
        for rec in LC['b100_id_recid_lookup_table'][LC['inverted_b100'][('100' + tag_ending), value]]:
            if LC['inverted_b100'][('100' + tag_ending, value)] in LC['brb100'][rec]['id']:
                for field_number in LC['brb100'][rec]['id'][LC['inverted_b100'][('100' + tag_ending, value)]]:
                    for key in LC['brb100'][rec]['fn'][field_number]:
                        if LC['b100'][key][0] == '100__a':
                            signatures.append((str(100), key, rec))

    if ('700' + tag_ending, value) in LC['inverted_b700']:
        for rec in LC['b700_id_recid_lookup_table'][LC['inverted_b700'][('700' + tag_ending), value]]:
            if LC['inverted_b700'][('700' + tag_ending, value)] in LC['brb700'][rec]['id']:
                for field_number in LC['brb700'][rec]['id'][LC['inverted_b700'][('700' + tag_ending, value)]]:
                    for key in LC['brb700'][rec]['fn'][field_number]:
                        if LC['b700'][key][0] == '700__a':
                            signatures.append((str(700), key, rec))
    return tuple(signatures)


def get_signatures_with_inspireID_cache(inspireID):
    return _get_signatures_with_tag_value_cache(inspireID, '__i')


def get_signatures_with_orcid_cache(orcid):
    return _get_signatures_with_tag_value_cache(orcid, '__j')


def get_all_recids_in_hepnames():
    return  set(perform_request_search(p='', cc='HepNames', rg=0))

get_all_recids_in_hepnames = memoized(get_all_recids_in_hepnames)

def get_inspireID_from_hepnames(pid):
    """return inspireID of a pid by searching the hepnames

    arguments:
    pid -- the pid of the author to search in the hepnames dataset
    """
    author_canonical_name = get_canonical_name_of_author(pid)
    hepnames_recids = get_all_recids_in_hepnames()
    try:
        #recid = perform_request_search(p="035:" + author_canonical_name[0][0], cc="HepNames")
        recid = set(search_unit_in_bibxxx(p=author_canonical_name[0][0], f='035__', type='='))
        recid = list(recid & hepnames_recids)

        if len(recid)>1:
            #raise Exception('TODO: more then one hepname with same BAI exception')
            return None

        hepname_record = get_record(recid[0])
        fields_dict = [dict(x[0]) for x in hepname_record['035']]
        for d in fields_dict:
            if '9' in d and d['9'] == 'INSPIRE':
                try:
                    return d['a']
                except KeyError:
                    raise BrokenHepNamesRecordException("Broken HepNames record", recid[0], 'INSPIREID')
    except IndexError:
        return None
    except KeyError:
        return None


def connect_hepnames_to_inspireID(pid, inspireID):
    """Connect the hepnames record with the record of the inspireID

    arguments:
    pid -- the pid of the author that has the inspireID
    inspireID -- the inspireID of the author
    """
    author_canonical_name = get_canonical_name_of_author(pid)
    # this should change
    # to an exception
    assert author_canonical_name
    recid = perform_request_search(p="035:" + inspireID, cc="HepNames")
    if recid:
        if len(recid) > 1:
            raise MultipleHepnamesRecordsWithSameIdException(
                "More than one hepnames record found with the same inspire id",
                recid,
                'INSPIREID')
        logger.log("Connecting pid", pid, "canonical_name", author_canonical_name, "inspireID", inspireID)
        add_cname_to_hepname_record(author_canonical_name, recid)


class Vacuumer(object):

    def __init__(self, pid):
        """Constructor of the class responsible for vacuuming the signatures to the right profile

        pid -- the pid of the author
        """
        self.claimed_paper_signatures = set(sig[1:4] for sig in get_papers_of_author(pid, include_unclaimed=False))
        self.unclaimed_paper_signatures = set(sig[1:4] for sig in get_papers_of_author(pid, include_claimed=False))
        self.claimed_paper_records = set(rec[2] for rec in self.claimed_paper_signatures)
        self.unclaimed_paper_records = set(rec[2] for rec in self.unclaimed_paper_signatures)
        self.pid = pid

    def vacuum_signature(self, signature):
        if signature not in self.unclaimed_paper_signatures and signature not in self.claimed_paper_signatures:
            if signature[2] in self.claimed_paper_records:
                raise DuplicateClaimedPaperException("Vacuum a duplicated claimed paper", self.pid, signature,
                                                     filter(lambda x: x[2] == signature[2], self.claimed_paper_signatures))

            duplicated_signatures = filter(lambda x: signature[2] == x[2], self.unclaimed_paper_signatures)

            if duplicated_signatures:
                logger.log("Conflict in pid ", self.pid, " with signature ", signature)
                new_pid = get_free_author_id()
                logger.log("Moving  conflicting signature ", duplicated_signatures[0], " from pid ", self.pid, " to pid ", new_pid)
                move_signature(duplicated_signatures[0], new_pid)
                # should or shouldn't
                # check after
                move_signature(signature, self.pid)
                after_vacuum = (sig[1:4] for sig in get_papers_of_author(self.pid))

                if signature not in after_vacuum:
                    move_signature(duplicated_signatures[0], self.pid)

                raise DuplicateUnclaimedPaperException("Vacuum a duplicated claimed paper", new_pid, signature, duplicated_signatures )

            logger.log("Hoovering ", signature, " to pid ", self.pid)
            move_signature(signature, self.pid)

def get_signatures_with_inspireID(inspireID):
    """get and vacuum of the signatures that belong to this inspireID

    arguments:
    inspireID -- the string containing the inspireID
    """
    logger.log("I was called with inspireID", inspireID)
    return get_signatures_with_inspireID_cache(inspireID)


def get_records_with_tag(tag):
    """return all the records with a specific tag

    arguments:
    tag -- the tag to search for
    """
    assert tag in ['100__i', '100__j', '700__i', '700__j']
    if tag.startswith("100"):
        return run_sql("select id_bibrec from bibrec_bib10x where id_bibxxx in (select id from bib10x where tag=%s)", (tag,))
    if tag.startswith("700"):
        return run_sql("select id_bibrec from bibrec_bib70x where id_bibxxx in (select id from bib70x where tag=%s)", (tag,))


def get_inspireID_from_claimed_papers(pid, intersection_set=None):
    """returns the inspireID found inside the claimed papers of the author.
    This happens only in case all the inspireIDs are the same,
    if there is  a conflict in the inspireIDs of the papers the
    ConflictingIdsFromReliableSource exception is raised

    arguments:
    pid -- the pid of the author
    intersection_set -- a set of paper signatures. The unclaimed paper
                        signatures are then intersected with this set.
                        the result is used for the inspireID search.
    """
    claimed_papers = get_papers_of_author(pid, include_unclaimed=False)
    if intersection_set:
        claimed_papers = filter(lambda x: x[3] in intersection_set, claimed_papers)
        # claimed_papers = [x for x in claimed_paper_signatures if x[3] in intersection_set]
    claimed_paper_signatures = (x[1:4] for x in claimed_papers)

    inspireID_list = []
    for sig in claimed_paper_signatures:
        inspireID = get_inspire_id_of_signature(sig)
        if inspireID:
            assert(len(inspireID) == 1)
            inspireID_list.append(inspireID[0])

    try:
        if inspireID_list[1:] == inspireID_list[:-1]:
            return inspireID_list[0]
    except IndexError:
        return None
    else:
        raise ConflictingIdsFromReliableSourceException('Claimed Papers', pid, 'INSPIREID', inspireID_list)


def get_inspireID_from_unclaimed_papers(pid, intersection_set=None):
    """returns the inspireID found inside the unclaimed papers of the author.
    This happens only in case all the inspireIDs are the same,
    if there is  a conflict in the inspireIDs of the papers the
    ConflictingIdsFromUnreliableSource exception is raised

    arguments:
    pid -- the pid of the author
    intersection_set -- a set of paper signatures. The unclaimed paper
                        signatures are then intersected with this set.
                        the result is used for the inspireID search.
    """
    unclaimed_papers = get_papers_of_author(pid, include_claimed=False)
    if intersection_set:
        unclaimed_papers = filter(lambda x: x[3] in intersection_set, unclaimed_papers)
    unclaimed_paper_signatures = (x[1:4] for x in unclaimed_papers)

    inspireID_list = []
    for sig in unclaimed_paper_signatures:
        inspireID = get_inspire_id_of_signature(sig)
        if inspireID:
            assert(len(inspireID) == 1)
            inspireID_list.append(inspireID[0])

    try:
        if inspireID_list[1:] == inspireID_list[:-1]:
            return inspireID_list[0]
    except IndexError:
        return None
    else:
        raise ConflictingIdsFromUnreliableSourceException('Unclaimed Papers', pid, 'INSPIREID', inspireID_list)


@timed
def hoover(authors=None):
    """Long description"""

    logger.log("Selecting records with identifiers...")
    recs = get_records_with_tag('100__i')
    recs += get_records_with_tag('100__j')
    recs += get_records_with_tag('700__i')
    recs += get_records_with_tag('700__j')
    logger.log("Found %s records" % len(recs))
    recs = set(recs) & set(run_sql("select DISTINCT(bibrec) from aidPERSONIDPAPERS"))
    logger.log("   out of owhich %s are in BibAuthorID" % len(recs))

    records_with_id = set(rec[0] for rec in recs)
    # records_with_id = [rec[0] for rec in set(recs)]

    destroy_partial_marc_caches()
    populate_partial_marc_caches(records_with_id, create_inverted_dicts=True)
    # same_ids = {}
    fdict_id_getters = {
        "INSPIREID": {
            'reliable': [get_inspire_id_of_author,
                         get_inspireID_from_hepnames,
                         lambda pid: get_inspireID_from_claimed_papers(
                         pid, intersection_set=records_with_id), ],

            'unreliable':
            [lambda pid: get_inspireID_from_unclaimed_papers(
             pid, intersection_set=records_with_id)],
            'signatures_getter': get_signatures_with_inspireID,
            'connection': connect_hepnames_to_inspireID,
            'data_dicts': {
                'pid_mapping': defaultdict(set),
                'id_mapping': defaultdict(set)
            }
        },

        "ORCID": {
            'reliable': [  # get_orcid_id_of_author,
                # get_inspireID_from_hepnames,
                # lambda pid: get_inspireID_from_claimed_papers(pid,
                # intersection_set=records_with_id)]
            ],

            'unreliable': [
                # get_inspireID_from_hepnames,
                # lambda pid: get_inspireID_from_claimed_papers(pid,
                # intersection_set=records_with_id)]
            ],
            'signatures_getter': lambda x: list(),
            'connection': lambda pid, _id: None,
            'data_dicts': {
                'pid_mapping': defaultdict(set),
                'id_mapping': defaultdict(set)
            }
        }
    }

    # change the names
    if not authors:
        authors = get_existing_authors()

    logger.log("Running on ", len(authors), " !")

    unclaimed_authors = defaultdict(set)

    for index, pid in enumerate(authors):
        logger.log("Searching for reliable ids of person %s" % pid)
        for identifier_type, functions in fdict_id_getters.iteritems():
            logger.log("    Type: %s" % identifier_type)

            G = (func(pid) for func in functions['reliable'])

            try:
                res = next((func for func in G if func), None)
            except ConflictingIdsFromReliableSourceException as e:
                open_rt_ticket(e)
                continue
            except BrokenHepNamesRecordException as e:
                open_rt_ticket(e)
                continue
            else:
                if res:
                    logger.log("   Found reliable id ", res)
                    fdict_id_getters[identifier_type]['data_dicts']['pid_mapping'][pid].add(res)
                    fdict_id_getters[identifier_type]['data_dicts']['id_mapping'][res].add(pid)
                else:
                    logger.log("   No reliable id found")
                    unclaimed_authors[identifier_type].add(pid)

    logger.log("Vacuuming reliable ids...")

    for identifier_type, data in fdict_id_getters.iteritems():
        for pid, identifiers in data['data_dicts']['pid_mapping'].iteritems():
            logger.log("   Person %s has reliable identifier(s) %s " % (str(pid),str(identifiers)))
            try:
                if len(identifiers) == 1:
                    identifier = list(identifiers)[0]
                    logger.log("        Considering  ", identifier)

                    if len(data['data_dicts']['id_mapping'][identifier]) == 1:
                        rowenta = Vacuumer(pid)
                        signatures = data['signatures_getter'](identifier)
                        logger.log("        Vacuuming %s signatures! " % str(len(signatures)))
                        for sig in signatures:
                            try:
                                rowenta.vacuum_signature(sig)
                            except DuplicateClaimedPaperException as e:
                                open_rt_ticket(e)
                            except DuplicateUnclaimedPaperException as e:
                                unclaimed_authors[identifier_type].add(e.pid)
                        logger.log("        Adding inspireid ", identifier, " to pid ", pid)
                        add_external_id_to_author(pid, identifier_type, identifier)
                        fdict_id_getters[identifier_type]['connection'](pid, identifier)

                    else:
                        raise MultipleAuthorsWithSameIdException(
                            "More than one authors with the same identifier",
                            data['data_dicts']['id_mapping'][identifier],
                            identifier)
                else:
                    raise MultipleIdsOnSingleAuthorException(
                        "More than one identifier on a single author ",
                        pid,
                        identifiers)

            except MultipleAuthorsWithSameIdException as e:
                open_rt_ticket(e)
            except MultipleIdsOnSingleAuthorException as e:
                open_rt_ticket(e)
            logger.log("   Done with ", pid)

    logger.log("Vacuuming unreliable ids...")
    print "unclaimed_authors"
    print unclaimed_authors['INSPIREID']

    for identifier_type, functions in fdict_id_getters.iteritems():
        for index, pid in enumerate(unclaimed_authors[identifier_type]):
            logger.log("Searching for urreliable ids of person %s" % pid)
            G = (func(pid) for func in functions['unreliable'])
            try:
                res = next((func for func in G if func), None)

                if res is None:
                    continue

            except ConflictingIdsFromUnreliableSourceException:
                open_rt_ticket(e)
                continue
            except BrokenHepNamesRecordException:
                open_rt_ticket(e)
                continue

            logger.log("   Person %s has unreliable identifier %s " % (str(pid),str(res)))

            if res in fdict_id_getters[identifier_type]['data_dicts']['id_mapping']:
                logger.log("        Id %s is already assigned to another person, skipping person %s " % (str(res), pid))
                continue

            rowenta = Vacuumer(pid)
            signatures = functions['signatures_getter'](res)

            for sig in signatures:
                try:
                    rowenta.vacuum_signature(sig)
                except DuplicateClaimedPaperException as e:
                    open_rt_ticket(e)
                except DuplicateUnclaimedPaperException as e:
                    pass

            logger.log("     Adding inspireid ", res, " to pid ", pid)
            add_external_id_to_author(pid, identifier_type, res)
            fdict_id_getters[identifier_type]['connection'](pid, res)
            logger.log("   Done with ", pid)

if __name__ == "__main__":
    logger.log("Initializing hoover")
    hoover()
    logger.log("Terminating hoover")
