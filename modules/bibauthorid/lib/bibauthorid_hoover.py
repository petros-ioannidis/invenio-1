import sys
from time import time
from invenio.search_engine import get_record, \
    perform_request_search
from invenio.dbquery import run_sql
from invenio.bibauthorid_dbinterface import get_existing_authors, \
    get_canonical_name_of_author, get_papers_of_author, get_all_paper_data_of_author, \
    get_signatures_of_paper, get_author_info_of_confirmed_paper, \
    _get_external_ids_from_papers_of_author, get_claimed_papers_of_author, \
    get_inspire_id_of_signature ,populate_partial_marc_caches, move_signature, \
    add_external_id_to_author, get_free_author_id, get_inspire_id_of_author, \
    get_orcid_id_of_author, destroy_partial_marc_caches
from invenio.bibauthorid_general_utils import memoized
import invenio.bibauthorid_dbinterface as db
from invenio.bibauthorid_webapi import get_hepnames, add_cname_to_hepname_record

try:
    from collections import defaultdict
except ImportError:
    from invenio.bibauthorid_general_utils import defaultdict

def timed(func):
    def print_time(*args, **kwargs):
        t0 = time()
        res = func(*args, **kwargs)
        print func.__name__, ': with args: ', args, kwargs, ' took: ', time()-t0
        return res
    return print_time

def get_signatures_with_inspireID_sql(inspireID):
    '''Signatures of specific inspireID using an Sql query'''
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
    '''Signatures of specific inspireID using CACHE'''
    signatures = []
    LC = db.MARC_100_700_CACHE
    if ('100'+tag_ending, value) in LC['inverted_b100']:
        for rec in LC['b100_id_recid_lookup_table'][LC['inverted_b100'][('100'+tag_ending), value]]:
            if LC['inverted_b100'][('100'+tag_ending, value)] in LC['brb100'][rec]['id']:
                for field_number in LC['brb100'][rec]['id'][LC['inverted_b100'][('100'+tag_ending, value)]]:
                    for key in LC['brb100'][rec]['fn'][field_number]:
                        if LC['b100'][key][0] == '100__a':
                            signatures.append((str(100), key ,rec))

    if ('700'+tag_ending, value) in LC['inverted_b700']:
        for rec in LC['b700_id_recid_lookup_table'][LC['inverted_b700'][('700'+tag_ending), value]]:
            if LC['inverted_b700'][('700'+tag_ending, value)] in LC['brb700'][rec]['id']:
                for field_number in LC['brb700'][rec]['id'][LC['inverted_b700'][('700'+tag_ending, value)]]:
                    for key in LC['brb700'][rec]['fn'][field_number]:
                        if LC['b700'][key][0] == '700__a':
                            signatures.append((str(700) ,key, rec))
    return tuple(signatures)

def get_signatures_with_inspireID_cache(inspireID):
    return _get_signatures_with_tag_value_cache(inspireID, '__i')

def get_signatures_with_orcid_cache(orcid):
    return _get_signatures_with_tag_value_cache(orcid, '__j')

def get_inspireID_from_hepnames(pid):
    '''return inspireID of a pid by searching the hepnames'''
    author_canonical_name = get_canonical_name_of_author(pid)

    if author_canonical_name:
        recid = perform_request_search(p="035:" + author_canonical_name[0][0], cc="HepNames")
        if recid:
            hepname_record = get_record(recid[0])
            if hepname_record:
                if '035' in hepname_record:
                    fields_dict = [dict(x[0]) for x in hepname_record['035']]
                    for d in fields_dict:
                        try:
                            if d['9'] == 'INSPIRE':
                                return d['a']
                        except KeyError, e:
                            print 'Oh look there is something wrong with this HepNames record! ', hepname_record
                            print 'Dictionary: ', fields_dict
    return None

def connect_hepnames_to_inspireID(pid, inspireID):
    '''return inspireID of a pid by searching the hepnames'''
    author_canonical_name = get_canonical_name_of_author(pid)
    if not author_canonical_name:
        return
    if inspireID:
        recid = perform_request_search(p="035:" + inspireID, cc="HepNames")
        if recid:
            if len(recid) > 1 :
                raise Exception("More than one hepnames record found with the same inspire id")
            hepname_record = get_record(recid[0])
            print "I am connecting pid",pid,"canonical_name",author_canonical_name,"inspireID",inspireID
            add_cname_to_hepname_record(author_canonical_name,recid)

def vacuum_signatures(pid, signatures, check_if_all_signatures_where_vacuumed=False):
    claimed_paper_signatures = set([sig[1:4] for sig in get_papers_of_author(pid, include_unclaimed=False)])
    unclaimed_paper_signatures = set([sig[1:4] for sig in get_papers_of_author(pid, include_claimed=False)])

    signatures_to_vacuum = (set(signatures) - unclaimed_paper_signatures) - claimed_paper_signatures

    claimed_paper_records = set(rec[2] for rec in claimed_paper_signatures)
    unclaimed_paper_records = set(rec[2] for rec in unclaimed_paper_signatures)
    for sig in signatures_to_vacuum:
        if sig[2] in claimed_paper_records:
            continue
            #raise Exception("Conflict in claimed papers")

        if sig[2] in unclaimed_paper_records:
            #move
            #create_new_author_by_signature(sig)
            print "Conflict in pid ",pid ," with signature ", sig
            new_pid = get_free_author_id()
            print "Moving  conflicting signature ",sig ," from pid ", pid, " to pid ", new_pid
            move_signature(sig, new_pid)
            #raise Exception("Conflict in unclaimed papers")
        print "Hoovering ",sig ," to pid ", pid
        move_signature(sig, pid)

    if check_if_all_signatures_where_vacuumed:
        paper_signatures = set([sig[1:4] for sig in get_papers_of_author(pid)])
        print "Paper_signatures", paper_signatures
        total_signatures = set(signatures)
        print "total_signatures", total_signatures
        total_signatures = total_signatures.union(unclaimed_paper_signatures).union(claimed_paper_signatures)
        print "Second total_signatures", total_signatures
        if paper_signatures == total_signatures:
            return True
        return False
    else:
        #we assume all signatures were vacuumed correctly and skip an expensive test.
        return True



def get_signatures_with_inspireID(inspireID):
    '''get and vacuum of the signatures that belong to this inspire id'''
    print "I was called with inspireID", inspireID
    return get_signatures_with_inspireID_cache(inspireID)
    #sql = timed(get_signatures_with_inspireID_sql)(inspireID)

def get_records_with_tag(tag):
    '''return all the records with a specific tag'''
    assert tag in ['100__i', '100__j', '700__i', '700__j']
    if tag.startswith("100"):
        return run_sql("select id_bibrec from bibrec_bib10x where id_bibxxx in (select id from bib10x where tag=%s)" ,(tag,))
    if tag.startswith("700"):
        return run_sql("select id_bibrec from bibrec_bib70x where id_bibxxx in (select id from bib70x where tag=%s)" ,(tag,))



#For both methods: the check for the id to be unique should be done outside of the function.
#Good old unix principle of do one thin and do it well...

def get_inspireID_from_claimed_papers(pid, intersection_set=None):
    claimed_paper_signatures = get_papers_of_author(pid, include_unclaimed=False)
    if intersection_set:
        #claimed_paper_signatures = set(claimed_paper_signatures) & intersection_set
        claimed_paper_signatures = [x for x in claimed_paper_signatures if x[3] in intersection_set]
    inspireID_list = []

    for sig in claimed_paper_signatures:
        inspireID = get_inspire_id_of_signature(sig[1:4])
        if inspireID:
            inspireID_list.append(inspireID[0])
        #if all ids are the same
    if inspireID_list:
        if inspireID_list[1:] == inspireID_list[:-1]:
            return inspireID_list[0]
        raise Exception("InspireID conflict")
    return None

def get_inspireID_from_unclaimed_papers(pid, intersection_set=None):
    unclaimed_paper_signatures = get_papers_of_author(pid, include_claimed=False)
    if intersection_set:
        #unclaimed_paper_signatures = set(unclaimed_paper_signatures) & intersection_set
        unclaimed_paper_signatures = [x for x in unclaimed_paper_signatures if x[3] in intersection_set]
    inspireID_list = []

    for sig in unclaimed_paper_signatures:
        inspireID = get_inspire_id_of_signature(sig[1:4])
        if inspireID:
            inspireID_list.append(inspireID[0])
        #if all ids are the same
    if inspireID_list:
        if inspireID_list[1:] == inspireID_list[:-1]:
            return inspireID_list[0]
    return None

def hoover(authors=None):
    '''Long description'''

    recs = get_records_with_tag('100__i')
    recs += get_records_with_tag('100__j')
    recs += get_records_with_tag('700__i')
    recs += get_records_with_tag('700__j')
    recs1 = run_sql("select DISTINCT(bibrec) from aidPERSONIDPAPERS")
    recs = set(recs) & set(recs1)
    records_with_id = set(rec[0] for rec in set(recs))
    #records_with_id = [rec[0] for rec in set(recs)]
    
    destroy_partial_marc_caches()
    populate_partial_marc_caches(records_with_id, create_inverted_dicts=True)
    #same_ids = {}
    fdict_id_getters = {
                        "INSPIREID": {
                                      'reliable': [get_inspire_id_of_author,
                                                   get_inspireID_from_hepnames,
                                                   lambda pid: get_inspireID_from_claimed_papers(pid, intersection_set=records_with_id),],

                                      'unreliable': [lambda pid: get_inspireID_from_unclaimed_papers(pid, intersection_set=records_with_id)],
                                      'signatures_getter': get_signatures_with_inspireID,
                                      'connection': connect_hepnames_to_inspireID,
                                      'data_dicts': { 
                                                      'pid_mapping': defaultdict(set),
                                                      'id_mapping': defaultdict(set)
                                                    }
                                     },

                        "ORCID":     {
                                      'reliable':   [  #get_orcid_id_of_author,
                                                       #get_inspireID_from_hepnames,
                                                       #lambda pid: get_inspireID_from_claimed_papers(pid, intersection_set=records_with_id)]
                                                    ],

                                      'unreliable': [
                                                       #get_inspireID_from_hepnames,
                                                       #lambda pid: get_inspireID_from_claimed_papers(pid, intersection_set=records_with_id)]
                                                    ],
                                      'signatures_getter': lambda x: list(),
                                      'connection': lambda pid, _id: pass
                                      'data_dicts': { 
                                      'data_dicts': { 
                                                      'pid_mapping': defaultdict(set),
                                                      'id_mapping': defaultdict(set)
                                                    }
                                    }

                       }



    #change the names

    if not authors:
        authors = get_existing_authors()
    print "running on ", len(authors)," !"

    unclaimed_authors = defaultdict(set) 
    reliable = True
    for index, pid in enumerate(authors):

        for identifier_type, functions in fdict_id_getters.iteritems():
            print "\npid ",pid
            G = (func(pid) for func in functions['reliable'])
            try:
                res = next((func for func in G if func), None)
                print "found reliable id", res
            except Exception, e:
                print 'Something went terribly wrong! ', str(e)
                continue

            if res:
                fdict_id_getters[identifier_type]['data_dicts']['pid_mapping'][pid].add(res)
                fdict_id_getters[identifier_type]['data_dicts']['id_mapping'][res].add(pid)
            else:
                unclaimed_authors[identifier_type].add(pid)
                continue

            assert res, 'res here should never be None'

    for identifier_type, data in fdict_id_getters.iteritems():
        for pid, identifiers in data['data_dicts']['pid_mapping'].iteritems():
            #check for duplication
            try:
                if len(identifiers) == 1:
                    identifier = list(identifiers)[0]
                    print "identifier", identifier
                    if len(data['data_dicts']['id_mapping'][identifier]) == 1:
                        signatures = data['signatures_getter'](identifier)
                        print "signatures", signatures
                        if vacuum_signatures(pid, signatures, check_if_all_signatures_where_vacuumed = reliable):
                            print "Adding inspireid ", identifier, " to pid ", pid
                            add_external_id_to_author(pid, identifier_type, identifier)
                            fdict_id_getters[identifier_type]['connection'](pid, identifier)

                    else:
                        raise Exception("More than one authors with the same identifier")
                else:
                    raise Exception("More than one identifier")
            except Exception, e:
                print 'Something went terribly wrong even here(reliable)! ', e
                continue

    print "we are entering the twilight zone"
    reliable = False
    for identifier_type, functions in fdict_id_getters.iteritems():
        for index, pid in enumerate(unclaimed_authors[identifier_type]):

            print "\npid ",pid
            G = (func(pid) for func in functions['unreliable'])
            try:
                res = next((func for func in G if func), None)
                print "found unreliable id", res
            except Exception, e:
                print 'Something went terribly wrong(unreliable)! ', str(e)
                continue

            if not res:
                continue

            assert res, 'res here should never be None'
            if res in fdict_id_getters[identifier_type]['data_dicts']['id_mapping']:
                print "Id",res," already there"
                print "skipping author",pid
                continue
            signatures = functions['signatures_getter'](res)
            try:
                if vacuum_signatures(pid, signatures, check_if_all_signatures_where_vacuumed = reliable):
                    print "Adding inspireid ", res, " to pid ", pid
                    add_external_id_to_author(pid, identifier_type, res)
                    fdict_id_getters[identifier_type]['connection'](pid, identifier)

            except Exception, e:
                print 'Something went terribly wrong even here(unreliable)! ', e
                continue

if __name__ == "__main__":
    print "Initializing hoover"
    timed(hoover)()
    print "Terminating hoover"
