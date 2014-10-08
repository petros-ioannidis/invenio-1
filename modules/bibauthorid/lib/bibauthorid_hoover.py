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
    get_orcid_id_of_author, destroy_partial_marc_caches, get_name_by_bibref
from invenio.bibauthorid_general_utils import memoized
import invenio.bibauthorid_dbinterface as db
import invenio.bibauthorid_config as bconfig
from invenio.bibauthorid_webapi import get_hepnames, add_cname_to_hepname_record
from invenio.bibcatalog import BIBCATALOG_SYSTEM
from invenio.bibauthorid_hoover_exceptions import *

logger = Logger('Hoover')

try:
    from collections import defaultdict
except ImportError:
    from invenio.bibauthorid_general_utils import defaultdict

def open_rt_ticket(e):
    """Take an exception e and, if allowed by the configuration, 
    open a ticket for that exception.

    Arguments:
    e -- the exception to be reported
    """
    global ticket_hashes
    ticket_hash = e.hash()
    subject = ticket_hash + ' ' + e.get_message_subject()
    body = e.get_message_body()
    debug = e.__repr__() + '\n' + '\n'.join([ str(key) + " " + str(value)  for key, value in vars(e).iteritems() ])
    if bconfig.HOOVER_OPEN_RT_TICKETS:
        queue = 'Test'
        if ticket_hash not in ticket_hashes.iterkeys():
            ticket_id = BIBCATALOG_SYSTEM.ticket_submit(uid=None, subject=subject, recordid=e.recid, text=body+'\n Debugging information: \n'+debug,
                                            queue=queue, priority="", owner="", requestor="")
            ticket_data = BIBCATALOG_SYSTEM.ticket_get_info(None, ticket_id)
            ticket_hashes[ticket_hash] = ticket_data, ticket_id, True
        else:
            ticket_hashes[ticket_hash] = ticket_hashes[ticket_hash][:2] + (True,)
            # If the ticket is already there check its status.  In case it is
            # marked as somehow solved -- i.e. resolved, deleted or rejected --
            # reopen it.
            if ticket_hashes[ticket_hash][0]['status'] in ['resolved', 'deleted', 'rejected']:
                BIBCATALOG_SYSTEM.ticket_set_attribute(None, ticket_hashes[ticket_hash][1], 'status', 'open')
    else:
        logger.log('sub: '+subject+'\nbody:\n'+body+'\ndbg:\n'+debug)

#this is to be deleted
def timed(func):
    def print_time(*args, **kwds):
        t0 = time()
        res = func(*args, **kwds)
        logger.log(func.__name__, ': with args: ', args, kwds, ' took: ', time() - t0)
        return res
    return print_time

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

    Arguments:
    pid -- the pid of the author to search in the hepnames dataset
    """
    author_canonical_name = get_canonical_name_of_author(pid)
    hepnames_recids = get_all_recids_in_hepnames()
    try:
        #recid = perform_request_search(p="035:" + author_canonical_name[0][0], cc="HepNames")
        recid = set(search_unit_in_bibxxx(p=author_canonical_name[0][0], f='035__', type='='))
        recid = list(recid & hepnames_recids)

        if len(recid) > 1:
            raise MultipleHepnamesRecordsWithSameIdException(
                "More than one hepnames record found with the same inspire id",
                recid,
                'INSPIREID')

        hepname_record = get_record(recid[0])
        fields_dict = [dict(x[0]) for x in hepname_record['035']]
        inspire_ids = []
        for d in fields_dict:
            if '9' in d and d['9'] == 'INSPIRE':
                try:
                    inspire_ids.append(d['a'])
                except KeyError:
                    raise BrokenHepNamesRecordException("There is no inspire id present, althought there is a MARC tag.", recid[0], 'INSPIREID')
        if len(inspire_ids) > 1:
            raise BrokenHepNamesRecordException("Multiple inspire ids found in the record.", recid[0], 'INSPIREID')
        else:
            return inspire_ids[0]
    except IndexError:
        return None
    except KeyError:
        return None

class HepnamesConnector(object):
    """A class to handle the connections that are to be performed.
    This is needed to avoid the creation of too many bibupload tasks

    Arguments:
    produce_connection_entry -- the function that returns the correspondance
                                between canonical name and record id

    Attributes:
    cname_dict -- the dictionary that holds the connections that need to be done
    """
    def __init__(self, produce_connection_entry=None, packet_size=1000, dry_hepnames_run=False):
        self.cname_dict = dict()
        self.produce_connection_entry = produce_connection_entry
        self.packet_size = packet_size
        if dry_hepnames_run:
            delattr(self, 'execute_connection')
            def _null_func():
                pass
            setattr(self, 'execute_connection', _null_func)

    def add_connection(self, pid, inspireID):
        tmp = connect_hepnames_to_inspireID(pid, inspireID)
        if tmp:
            self.cname_dict.update(tmp)
        if len(self.cname_dict.keys()) >= self.packet_size:
            self.execute_connection()

    def execute_connection(self):
        add_cname_to_hepname_record(self.cname_dict)
        self.cname_dict.clear()

def connect_hepnames_to_inspireID(pid, inspireID):
    """Connect the hepnames record with the record of the inspireID

    Arguments:
    pid -- the pid of the author that has the inspireID
    inspireID -- the inspireID of the author
    """
    author_canonical_name = get_canonical_name_of_author(pid)

    if not author_canonical_name:
        #TODO: signal that something is wrong instead of just ignoring. Ignoring is safe for the moment.
        return None

    recid = perform_request_search(p="035:" + inspireID, cc="HepNames")
    if recid:
        if len(recid) > 1:
            raise MultipleHepnamesRecordsWithSameIdException(
                "More than one hepnames record found with the same inspire id",
                recid,
                'INSPIREID')
        logger.log("Connecting pid", pid, "canonical_name", author_canonical_name, "inspireID", inspireID)
        return {author_canonical_name: recid[0]}


class Vacuumer(object):
    """Class responsible for vacuuming the signatures to the right profile

    Constructor arguments:
    pid -- the pid of the author
    """
    def __init__(self, pid):
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
                move_signature(signature, self.pid)
                after_vacuum = (sig[1:4] for sig in get_papers_of_author(self.pid))

                if signature not in after_vacuum:
                    move_signature(duplicated_signatures[0], self.pid)

                raise DuplicateUnclaimedPaperException("Vacuum a duplicated unclaimed paper", new_pid, signature, duplicated_signatures )

            logger.log("Hoovering ", signature, " to pid ", self.pid)
            move_signature(signature, self.pid)

def get_signatures_with_inspireID(inspireID):
    """get and vacuum of the signatures that belong to this inspireID

    Arguments:
    inspireID -- the string containing the inspireID
    """
    logger.log("I was called with inspireID", inspireID)
    return get_signatures_with_inspireID_cache(inspireID)


def get_records_with_tag(tag):
    """return all the records with a specific tag

    Arguments:
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

    Arguments:
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
            if len(inspireID) > 1:
                open_rt_ticket(ConflictingIdsOnRecordException('Conflicting ids found', pid, 'INSPIREID', inspireID, sig[2]))
                return None

            inspireID_list.append(inspireID[0])

    try:
        if inspireID_list[1:] == inspireID_list[:-1]:
            return inspireID_list[0]
    except IndexError:
        return None
    else:
        raise MultipleIdsOnSingleAuthorException('Signatures conflicting:' + ','.join(claimed_paper_signatures), pid, 'INSPIREID', inspireID_list)


def get_inspireID_from_unclaimed_papers(pid, intersection_set=None):
    """returns the inspireID found inside the unclaimed papers of the author.
    This happens only in case all the inspireIDs are the same,
    if there is  a conflict in the inspireIDs of the papers the
    ConflictingIdsFromUnreliableSource exception is raised

    Arguments:
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
            if len(inspireID) > 1:
                open_rt_ticket(ConflictingIdsOnRecordException('Conflicting ids found', pid, 'INSPIREID', inspireID, sig[2]))
                return None

            inspireID_list.append(inspireID[0])

    try:
        if inspireID_list[1:] == inspireID_list[:-1]:
            return inspireID_list[0]
    except IndexError:
        return None
    else:
        raise MultipleIdsOnSingleAuthorException('Signatures conflicting:' + ','.join(unclaimed_paper_signatures), pid, 'INSPIREID', inspireID_list)

ticket_hashes = dict()

#put packet_size inside the daemon
@timed
def hoover(authors=None, check_db_consistency=False, dry_run=False, packet_size=1000, dry_hepnames_run=False, statistics=False):
    """The actions that hoover performs are the following:
    1. Find out the identifiers that belong to the authors(pids) in the database
    2. Find and pull all the signatures that have the same identifier as the author to the author
    3. Connect the profile of the author with the hepnames collection entry
    (optional). check the database to see if it is in a consistent state

    Keyword arguments:
    authors -- an iterable of authors to be hoovered
    check_db_consistency -- perform checks for the consistency of th database
    dry_run -- do not alter the database tables
    packet_size -- squeeze together the marcxml. This there are fewer bibupload 
                   processes for the bibsched to run.
    dry_hepnames_run -- do not alter the hepnames collection
    statistics -- report statistics for the algorithm (to be done)
    """
    
    logger.log("Packet size %d" % packet_size)
    logger.log("Initializing hoover")
    logger.log("Selecting records with identifiers...")
    recs = get_records_with_tag('100__i')
    recs += get_records_with_tag('100__j')
    recs += get_records_with_tag('700__i')
    recs += get_records_with_tag('700__j')
    logger.log("Found %s records" % len(recs))
    recs = set(recs) & set(run_sql("select DISTINCT(bibrec) from aidPERSONIDPAPERS"))
    logger.log("   out of owhich %s are in BibAuthorID" % len(recs))

    records_with_id = set(rec[0] for rec in recs)

    destroy_partial_marc_caches()
    populate_partial_marc_caches(records_with_id, create_inverted_dicts=True)

    if bconfig.HOOVER_OPEN_RT_TICKETS:
        global ticket_hashes
        logger.log("Ticketing system rt is used")
        logger.log("Building hash cache for tickets")
        ticket_ids = BIBCATALOG_SYSTEM.ticket_search(None, subject='[Hoover]')
        print ticket_ids
        for ticket_id in ticket_ids:
            print ticket_id
            try:
                ticket_data = BIBCATALOG_SYSTEM.ticket_get_info(None, ticket_id)
                ticket_hashes[ticket_data['subject'].split()[0]] = ticket_data, ticket_id, False
            except IndexError:
                logger.log("Problem in subject of ticket", ticket_id)
        print ticket_hashes
        logger.log("Found %s tickets" % len(ticket_hashes))

    fdict_id_getters = {
        "INSPIREID": {
            'reliable': [get_inspire_id_of_author,
                         get_inspireID_from_hepnames,
                         lambda pid: get_inspireID_from_claimed_papers(
                         pid, intersection_set=records_with_id)],

            'unreliable': [lambda pid: get_inspireID_from_unclaimed_papers(
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
    hep_connector = HepnamesConnector(packet_size=packet_size, dry_hepnames_run=dry_hepnames_run)

    for index, pid in enumerate(authors):
        logger.log("Searching for reliable ids of person %s" % pid)
        for identifier_type, functions in fdict_id_getters.iteritems():
            logger.log("    Type: %s" % identifier_type)

            try:
                G = (func(pid) for func in functions['reliable'])
                if check_db_consistency:
                    results = filter(None, (func for func in G if func))
                    try:
                        #check if this is reduntant
                        if len(results) == 1:
                            consistent_db = True
                        else:
                            consistent_db = len(set(results)) <= 1
                        res = results[0]
                    except IndexError:
                        res = None
                    else:
                        if consistent_db == False:
                            res = None
                            raise InconsistentIdentifiersException('Inconsistent database', pid, identifier_type, set(results))
                else:
                    res = next((func for func in G if func), None)
            except MultipleIdsOnSingleAuthorException as e:
                open_rt_ticket(e)
            except BrokenHepNamesRecordException as e:
                open_rt_ticket(e)
            except InconsistentIdentifiersException as e:
                open_rt_ticket(e)
            except MultipleHepnamesRecordsWithSameIdException as e:
                open_rt_ticket(e)
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
        hep_connector.produce_connection_entry = fdict_id_getters[identifier_type]['connection']
        for pid, identifiers in data['data_dicts']['pid_mapping'].iteritems():
            logger.log("   Person %s has reliable identifier(s) %s " % (str(pid),str(identifiers)))
            try:
                if len(identifiers) == 1:
                    identifier = list(identifiers)[0]
                    logger.log("        Considering  ", identifier)

                    if len(data['data_dicts']['id_mapping'][identifier]) == 1:
                        if not dry_run:
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
                            hep_connector.add_connection(pid, identifier)

                    else:
                        raise MultipleAuthorsWithSameIdException(
                            "More than one authors with the same identifier",
                            data['data_dicts']['id_mapping'][identifier],
                            identifier)
                else:
                    raise MultipleIdsOnSingleAuthorException(
                        "More than one identifier on a single author ",
                        pid,
                        'INSPIREID',
                        identifiers)

            except MultipleAuthorsWithSameIdException as e:
                open_rt_ticket(e)
            except MultipleIdsOnSingleAuthorException as e:
                open_rt_ticket(e)
            except MultipleHepnamesRecordsWithSameIdException as e:
                open_rt_ticket(e)
            logger.log("   Done with ", pid)

    logger.log("Vacuuming unreliable ids...")

    for identifier_type, functions in fdict_id_getters.iteritems():
        hep_connector.produce_connection_entry = fdict_id_getters[identifier_type]['connection']
        for index, pid in enumerate(unclaimed_authors[identifier_type]):
            logger.log("Searching for unreliable ids of person %s" % pid)
            try:
                G = (func(pid) for func in functions['unreliable'])
                res = next((func for func in G if func), None)
                if res is None:
                    continue
            except MultipleIdsOnSingleAuthorException as e:
                # For the beginning, we want to ignore this (it would open a lot of tickets which are almost impossible to
                # fix manually)
                # open_rt_ticket(e)
                continue
            except BrokenHepNamesRecordException as e:
                open_rt_ticket(e)
                continue
            except MultipleHepnamesRecordsWithSameIdException as e:
                open_rt_ticket(e)

            logger.log("   Person %s has unreliable identifier %s " % (str(pid),str(res)))

            if res in fdict_id_getters[identifier_type]['data_dicts']['id_mapping']:
                logger.log("        Id %s is already assigned to another person, skipping person %s " % (str(res), pid))
                continue

            if not dry_run:
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
                hep_connector.add_connection(pid, res)
            logger.log("   Done with ", pid)
    hep_connector.execute_connection()
    for ticket in ticket_hashes:
        if ticket[2] == False:
            BIBCATALOG_SYSTEM.ticket_set_attribute(None, ticket[1], 'status', 'resolved')
    logger.log("Terminating hoover")

if __name__ == "__main__":
    hoover(check_db_consistency=True)
