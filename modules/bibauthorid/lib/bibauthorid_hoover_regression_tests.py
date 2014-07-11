from unittest import *
import os

from invenio.testutils import make_test_suite
from invenio.testutils import run_test_suite

from invenio.bibauthorid_testutils import *

from invenio.bibtask import setup_loggers
from invenio.bibtask import task_set_task_param
from invenio.bibtask import task_low_level_submission

from invenio.bibupload_regression_tests import wipe_out_record_from_all_tables
from invenio.dbquery import run_sql

import invenio.bibauthorid_rabbit
from invenio.bibauthorid_rabbit import rabbit

import invenio.bibauthorid_hoover
from invenio.bibauthorid_hoover import hoover

from invenio.bibauthorid_dbinterface import get_inspire_id_of_author
from invenio.bibauthorid_dbinterface import _delete_from_aidpersonidpapers_where
from invenio.bibauthorid_dbinterface import get_papers_of_author

from invenio.search_engine import get_record

def index_hepnames_authors():
    """runs bibindex for the index '_type' and returns the task_id"""
    program = os.path.join(CFG_BINDIR, 'bibindex')
    #Add hepnames collection
    task_id = task_low_level_submission('bibindex', 'hoover_regression_tests', '-w', 'author', '-u', 'admin', )
    COMMAND = "%s %s > /dev/null 2> /dev/null" % (program, str(task_id))
    os.system(COMMAND)
    return task_id

def clean_up_the_database(inspireID):
    if inspireID:
        run_sql("delete from aidPERSONIDDATA where data=%s", (inspireID,))

class BibAuthorIDHooverTestCase(TestCase):

    run_exec = False

    @classmethod
    def setUpClass(cls):
        print 'Init'


        #print dir(cls)
        #if 'run' in dir(cls) and cls.run:
            #return
        if cls.run_exec:
            print 'I am not defined'
            return
        cls.run_exec = True
        cls.verbose = 0
        cls.logger = setup_loggers()
        cls.logger.info('Setting up regression tests...')
        task_set_task_param('verbose', cls.verbose)

        cls.authors = {  'author1': {
                                        'name': 'authoraaaaa authoraaaab',
                                        'inspireID': 'INSPIRE-FAKE_ID1'},
                         'author2': {
                                        'name': 'authorbbbba authorbbbbb',
                                        'inspireID': 'INSPIRE-FAKE_ID2'},
                         'author3': {
                                        'name': 'authorcccca authorccccb',
                                        'inspireID': 'INSPIRE-FAKE_ID3'},
                         'author4': {
                                        'name': 'authordddda authorddddb',
                                        'inspireID': 'INSPIRE-FAKE_ID4'},
                         'author5': {
                                        'name': 'authoreeeea authoreeeeb',
                                        'inspireID': 'INSPIRE-FAKE_ID5'},
                         'author6': {
                                        'name': 'authorffffa authorffffb',
                                        'inspireID': 'INSPIRE-FAKE_ID6'},
                         'author7': {
                                        'name': 'authorgggga authorggggb',
                                        'inspireID': 'INSPIRE-FAKE_ID7'},
                         'author8': {
                                        'name': 'authorhhhha authorhhhhb',
                                        'inspireID': 'INSPIRE-FAKE_ID8'},
                         'author9': {
                                        'name': 'authoriiiia authoriiiib',
                                        'inspireID': 'INSPIRE-FAKE_ID9'},
                         'author10': {
                                        'name': 'authorjjjja authorjjjjb',
                                        'inspireID': 'INSPIRE-FAKE_ID10'},
                         'author11': {
                                        'name': 'authorkkkka authorkkkkb',
                                        'inspireID': 'INSPIRE-FAKE_ID11'},
                         'author12': {
                                        'name': 'authorlllla authorllllb',
                                        'inspireID': 'INSPIRE-FAKE_ID12'},
                         'author13': {
                                        'name': 'authormmmma authormmmmb',
                                        'inspireID': 'INSPIRE-FAKE_ID13'},
                         'author14': {
                                        'name': 'authornnnna authornnnnb',
                                        'inspireID': 'INSPIRE-FAKE_ID14'},
                         'author15': {
                                        'name': 'authorooooa authoroooob',
                                        'inspireID': 'INSPIRE-FAKE_ID15'}
                       }
        cls.marc_xmls = dict()
        cls.bibrecs = dict()
        cls.pids = dict()
        cls.bibrefs = dict()

        def set_up_test_hoover_inertia():
            cls.marc_xmls['paper1'] = get_new_marc_for_test(cls.authors['author1']['name'])
            cls.bibrecs['paper1'] = get_bibrec_for_record(cls.marc_xmls['paper1'], opt_mode='insert')
            cls.marc_xmls['paper1'] = add_001_field(cls.marc_xmls['paper1'], cls.bibrecs['paper1'])

        def set_up_test_hoover_duplication():
            cls.marc_xmls['paper2'] = get_new_marc_for_test(cls.authors['author2']['name'], None, \
                                                             ((cls.authors['author2']['inspireID'], 'i'),))

            cls.bibrecs['paper2'] = get_bibrec_for_record(cls.marc_xmls['paper2'], opt_mode='insert')
            cls.marc_xmls['paper2'] = add_001_field(cls.marc_xmls['paper2'], cls.bibrecs['paper2'])

        def set_up_test_hoover_assign_one_inspire_id_from_an_unclaimed_paper():
            cls.marc_xmls['paper3'] = get_new_marc_for_test(cls.authors['author3']['name'], None, \
                                                             ((cls.authors['author3']['inspireID'], 'i'),))

            cls.bibrecs['paper3'] = get_bibrec_for_record(cls.marc_xmls['paper3'], opt_mode='insert')
            cls.marc_xmls['paper3'] = add_001_field(cls.marc_xmls['paper3'], cls.bibrecs['paper3'])

        def set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper():
            cls.marc_xmls['paper4'] = get_new_marc_for_test(cls.authors['author4']['name'], None, \
                                                             ((cls.authors['author4']['inspireID'], 'i'),))

            cls.bibrecs['paper4'] = get_bibrec_for_record(cls.marc_xmls['paper4'], opt_mode='insert')
            cls.marc_xmls['paper4'] = add_001_field(cls.marc_xmls['paper4'], cls.bibrecs['paper4'])

        def set_up_test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID():
            cls.marc_xmls['paper5'] = get_new_marc_for_test(cls.authors['author5']['name'], None, \
                                                             ((cls.authors['author5']['inspireID'], 'i'),))

            cls.bibrecs['paper5'] = get_bibrec_for_record(cls.marc_xmls['paper5'], opt_mode='insert')
            cls.marc_xmls['paper5'] = add_001_field(cls.marc_xmls['paper5'], cls.bibrecs['paper5'])

            cls.marc_xmls['paper6'] = get_new_marc_for_test(cls.authors['author5']['name'], None, \
                                                             ((cls.authors['author6']['inspireID'], 'i'),))

            cls.bibrecs['paper6'] = get_bibrec_for_record(cls.marc_xmls['paper6'], opt_mode='insert')
            cls.marc_xmls['paper6'] = add_001_field(cls.marc_xmls['paper6'], cls.bibrecs['paper6'])

        def set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID():
            cls.marc_xmls['paper7'] = get_new_marc_for_test(cls.authors['author7']['name'], None, \
                                                             ((cls.authors['author7']['inspireID'], 'i'),))

            cls.bibrecs['paper7'] = get_bibrec_for_record(cls.marc_xmls['paper7'], opt_mode='insert')
            cls.marc_xmls['paper7'] = add_001_field(cls.marc_xmls['paper7'], cls.bibrecs['paper7'])

            cls.marc_xmls['paper8'] = get_new_marc_for_test(cls.authors['author7']['name'], None, \
                                                             ((cls.authors['author8']['inspireID'], 'i'),))

            cls.bibrecs['paper8'] = get_bibrec_for_record(cls.marc_xmls['paper8'], opt_mode='insert')
            cls.marc_xmls['paper8'] = add_001_field(cls.marc_xmls['paper8'], cls.bibrecs['paper8'])

        def set_up_test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID():
            cls.marc_xmls['paper9'] = get_new_marc_for_test(cls.authors['author9']['name'], None, \
                                                             ((cls.authors['author2']['inspireID'], 'i'),))

            cls.bibrecs['paper9'] = get_bibrec_for_record(cls.marc_xmls['paper9'], opt_mode='insert')
            cls.marc_xmls['paper9'] = add_001_field(cls.marc_xmls['paper9'], cls.bibrecs['paper9'])

            cls.marc_xmls['paper10'] = get_new_marc_for_test(cls.authors['author9']['name'], None, \
                                                             ((cls.authors['author10']['inspireID'], 'i'),))

            cls.bibrecs['paper10'] = get_bibrec_for_record(cls.marc_xmls['paper10'], opt_mode='insert')
            cls.marc_xmls['paper10'] = add_001_field(cls.marc_xmls['paper10'], cls.bibrecs['paper10'])

        def set_up_test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper():
            cls.marc_xmls['paper11'] = get_new_marc_for_test(cls.authors['author11']['name'], None, \
                                                             ((cls.authors['author11']['inspireID'], 'i'),))

            cls.bibrecs['paper11'] = get_bibrec_for_record(cls.marc_xmls['paper11'], opt_mode='insert')
            cls.marc_xmls['paper11'] = add_001_field(cls.marc_xmls['paper11'], cls.bibrecs['paper11'])

            cls.marc_xmls['paper12'] = get_new_marc_for_test(cls.authors['author12']['name'], None, \
                                                             ((cls.authors['author11']['inspireID'], 'i'),))

            cls.bibrecs['paper12'] = get_bibrec_for_record(cls.marc_xmls['paper12'], opt_mode='insert')
            cls.marc_xmls['paper12'] = add_001_field(cls.marc_xmls['paper12'], cls.bibrecs['paper12'])

        def set_up_test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper():
            cls.marc_xmls['paper13'] = get_new_marc_for_test(cls.authors['author13']['name'], None, \
                                                             ((cls.authors['author13']['inspireID'], 'i'),))

            cls.bibrecs['paper13'] = get_bibrec_for_record(cls.marc_xmls['paper13'], opt_mode='insert')
            cls.marc_xmls['paper13'] = add_001_field(cls.marc_xmls['paper13'], cls.bibrecs['paper13'])

            cls.marc_xmls['paper14'] = get_new_marc_for_test(cls.authors['author14']['name'], None, \
                                                             ((cls.authors['author13']['inspireID'], 'i'),))

            cls.bibrecs['paper14'] = get_bibrec_for_record(cls.marc_xmls['paper14'], opt_mode='insert')
            cls.marc_xmls['paper14'] = add_001_field(cls.marc_xmls['paper14'], cls.bibrecs['paper14'])

        def set_up_test_hoover_assign_one_inspire_id_from_hepnames_record():
            cls.marc_xmls['paper15'] = get_new_hepnames_marc_for_test(cls.authors['author15']['name'], ((cls.authors['author15']['inspireID'], 'i'),))

            cls.bibrecs['paper15'] = get_bibrec_for_record(cls.marc_xmls['paper15'], opt_mode='insert')
            cls.marc_xmls['paper15'] = add_001_field(cls.marc_xmls['paper15'], cls.bibrecs['paper15'])


        set_up_test_hoover_inertia()
        set_up_test_hoover_duplication()
        set_up_test_hoover_assign_one_inspire_id_from_an_unclaimed_paper()
        set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper()
        set_up_test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID()
        set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID()
        set_up_test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID()
        set_up_test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper()
        set_up_test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper()
        set_up_test_hoover_assign_one_inspire_id_from_hepnames_record()

        cls.bibrecs_to_clean = [cls.bibrecs[key] for key in cls.bibrecs]
        rabbit([cls.bibrecs[key] for key in cls.bibrecs], verbose=False)
        print cls.bibrecs

        for key in cls.authors:
            temp = set()
            cls.bibrefs[key] = get_bibref_value_for_name(cls.authors[key]['name'])
            temp = run_sql("select personid from aidPERSONIDPAPERS where bibref_value=%s and bibrec=%s and name=%s", (cls.bibrefs[key], cls.bibrecs[key.replace('author','paper')], cls.authors[key]['name']))
            cls.pids[key] = temp[0][0] if temp else ()
        print cls.bibrefs
        print cls.pids

        claim_test_paper(cls.bibrecs['paper4'])
        claim_test_paper(cls.bibrecs['paper7'])
        claim_test_paper(cls.bibrecs['paper9'])
        claim_test_paper(cls.bibrecs['paper10'])
        claim_test_paper(cls.bibrecs['paper11'])
        claim_test_paper(cls.bibrecs['paper13'])
        claim_test_paper(cls.bibrecs['paper14'])
        #print list(set([cls.pids[key] for key in cls.pids]))
        print list(set(cls.pids[key] for key in cls.pids))
        hoover(list(set(cls.pids[key] for key in cls.pids if cls.pids[key])))


    @classmethod
    def tearDownClass(cls):

        # All records are wiped out for consistency.
        print "I am cleaning"
        clean_up_the_database(cls.authors['author1']['inspireID'])
        clean_up_the_database(cls.authors['author2']['inspireID'])
        clean_up_the_database(cls.authors['author3']['inspireID'])
        clean_up_the_database(cls.authors['author4']['inspireID'])
        clean_up_the_database(cls.authors['author5']['inspireID'])
        clean_up_the_database(cls.authors['author6']['inspireID'])
        clean_up_the_database(cls.authors['author7']['inspireID'])
        clean_up_the_database(cls.authors['author8']['inspireID'])
        clean_up_the_database(cls.authors['author9']['inspireID'])
        clean_up_the_database(cls.authors['author10']['inspireID'])
        clean_up_the_database(cls.authors['author11']['inspireID'])
        clean_up_the_database(cls.authors['author12']['inspireID'])
        clean_up_the_database(cls.authors['author13']['inspireID'])
        clean_up_the_database(cls.authors['author14']['inspireID'])
        clean_up_the_database(cls.authors['author15']['inspireID'])
        
        for key in cls.pids:
            if cls.pids[key]:
                _delete_from_aidpersonidpapers_where(cls.pids[key])

        for bibrec in cls.bibrecs_to_clean:
            wipe_out_record_from_all_tables(bibrec)
            clean_authors_tables(bibrec)

class OneAuthorOnePaperHooverTestCase(BibAuthorIDHooverTestCase):

    @classmethod
    def setUpClass(self):
        BibAuthorIDHooverTestCase.setUpClass()

    @classmethod
    def tearDownClass(self):
        pass 

    def test_hoover_one_author_one_paper(self):

        def test_hoover_inertia():
            '''If nothing should change then nothing changes'''

            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author1'])
            self.assertEquals(inspireID, tuple())

        def test_hoover_for_duplication():
            '''No duplicated information in the database'''

            #author_papers = get_papers_of_author(self.pids['author1'])
            #inspire_list_after = run_sql("select * from aidPERSONIDDATA where tag='extid:INSPIREID' and data=%s",(inspireID_after,))
            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author2'])
            self.assertEquals(inspireID, 'INSPIRE-FAKE_ID2')
            #self.assertEquals(author_papers_before, author_papers_after)

        def test_hoover_assign_one_inspire_id_from_an_unclaimed_paper():
            '''
            Preconditions:
                *This is the only paper that the author has
                *No other author has a claim on the paper
            Postconditions:
                *connect author with inspireID taken from the unclaimed paper
            '''

            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author3'])
            self.assertEquals(inspireID, BibAuthorIDHooverTestCase.authors['author3']['inspireID'])

        def test_hoover_assign_one_inspire_id_from_a_claimed_paper():
            '''
            Preconditions:
                *This is the only paper that the author has
                *No other author has a claim on the paper
            Postconditions:
                *connect author with inspireID taken from the claimed paper
            '''

            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author4'])
            self.assertEquals(inspireID, BibAuthorIDHooverTestCase.authors['author4']['inspireID'])

        test_hoover_inertia()
        test_hoover_for_duplication()
        test_hoover_assign_one_inspire_id_from_an_unclaimed_paper()
        test_hoover_assign_one_inspire_id_from_a_claimed_paper()

class OneAuthorManyPapersHooverTestCase(BibAuthorIDHooverTestCase):

    #def setUp(self):
        #super(OneAuthorManyPapersHooverTestCase, self).setUp()

    #def tearDown(self):
        #super(OneAuthorManyPapersHooverTestCase, self).tearDown()

    #def setUpClass(self):
        #super(OneAuthorOnePaperHooverTestCase, self).setUpClass()

    #def tearDownClass(self):
        #super(OneAuthorOnePaperHooverTestCase, self).tearDownClass()

    @classmethod
    def setUpClass(self):
        BibAuthorIDHooverTestCase.setUpClass()

    @classmethod
    def tearDownClass(self):
        pass 
    #@classmethod
    #def tearDownClass(self):
        #BibAuthorIDHooverTestCase.tearDownClass()

    def test_hoover_one_author_many_papers(self):

        def test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID():
            '''
            Preconditions:
                *One unclaimed paper of the author with inspireID: INSPIRE-FAKE_ID1
                *One unclaimed paper of the author with inspireID: INSPIRE-FAKE_ID2
                *The author has no inspireID connected to him
                *No other author has a claim on the papers

            Postconditions:
                *Nothing has changed
            '''

            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author5'])
            self.assertEquals(inspireID, ())

        def test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID():
            '''
            Preconditions:
                *One claimed paper of the author with inspireID: INSPIRE-FAKE_ID1
                *One unclaimed paper of the author with inspireID: INSPIRE-FAKE_ID2
                *The author has no inspireID connected to him
                *No other author has a claim on the papers

            Postconditions:
                *connect author with inspireID taken from the claimed paper(INSPIRE-FAKE_ID1)
            '''

            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author7'])
            print BibAuthorIDHooverTestCase.pids['author7']
            print inspireID
            self.assertEquals(inspireID, BibAuthorIDHooverTestCase.authors['author7']['inspireID'])

        def test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID():
            '''
            Preconditions:
                *One claimed paper of the author with inspireID: INSPIRE-FAKE_ID1
                *One claimed paper of the author with inspireID: INSPIRE-FAKE_ID2
                *The author has no inspireID connected to him
                *No other author has a claim on the papers

            Postconditions:
                *Nothing has changed
            '''

            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author9'])
            self.assertEquals(inspireID, ())

        test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID()
        test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID()
        test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID()

class ManyAuthorsHooverTestCase(BibAuthorIDHooverTestCase):

    #def setUp(self):
        #super(ManyAuthorsHooverTestCase, self).setUp()

    #def tearDown(self):
        #super(ManyAuthorsHooverTestCase, self).tearDown()
    #def setUpClass(self):
        #super(OneAuthorOnePaperHooverTestCase, self).setUpClass()

    #def tearDownClass(self):
        #super(OneAuthorOnePaperHooverTestCase, self).tearDownClass()

    @classmethod
    def setUpClass(self):
        BibAuthorIDHooverTestCase.setUpClass()

    @classmethod
    def tearDownClass(self):
        BibAuthorIDHooverTestCase.tearDownClass()
    #@classmethod
    #def tearDownClass(self):
        #BibAuthorIDHooverTestCase.tearDownClass()

    def test_many_authors(self):

        def test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper():
            '''
            Preconditions:
                *One claimed paper of the author1 with inspireID: INSPIRE-FAKE_ID1
                *One unclaimed paper of the author2 with inspireID: INSPIRE-FAKE_ID1
                *The authors has no inspireIDs connected to them
                *No other authors has a claim on the papers

            Postconditions:
                *Author1 is connected to the inspireID: INSPIRE-FAKE_ID1
                *The unclaimed paper of author2 is now moved to author1
            '''

            first_author_papers = get_papers_of_author(BibAuthorIDHooverTestCase.pids['author11'])
            second_author_papers = get_papers_of_author(BibAuthorIDHooverTestCase.pids['author12'])

            inspireID1 = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author11'])
            self.assertEquals(inspireID1, BibAuthorIDHooverTestCase.authors['author11']['inspireID'])
            inspireID2 = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author12'])
            self.assertEquals(inspireID2, ())

            self.assertEquals(len(first_author_papers), 2)
            self.assertEquals(len(second_author_papers), 0)

        def test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper():
            '''
            Preconditions:
                *One claimed paper of the author1 with inspireID: INSPIRE-FAKE_ID1
                *One claimed paper of the author2 with inspireID: INSPIRE-FAKE_ID1
                *The authors has no inspireIDs connected to them
                *No other authors has a claim on the papers

            Postconditions:
                *Author1 is connected to the inspireID: INSPIRE-FAKE_ID1
                *Nothing else changes
            '''


            first_author_papers = get_papers_of_author(BibAuthorIDHooverTestCase.pids['author13'])
            second_author_papers = get_papers_of_author(BibAuthorIDHooverTestCase.pids['author14'])

            inspireID1 = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author13'])
            self.assertEquals(inspireID1, ())
            inspireID2 = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author14'])
            self.assertEquals(inspireID2, ())

            self.assertEquals(len(first_author_papers), 1)
            self.assertEquals(len(second_author_papers), 1)

        test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper()
        test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper()

class HepnamesHooverTestCase(BibAuthorIDHooverTestCase):
    @classmethod
    def setUpClass(self):
        BibAuthorIDHooverTestCase.setUpClass()

    @classmethod
    def tearDownClass(self):
        BibAuthorIDHooverTestCase.tearDownClass()

    def test_hepnames(self):
        def test_hoover_assign_one_inspire_id_from_hepnames_record():
            inspireID = get_inspire_id_of_author(BibAuthorIDHooverTestCase.pids['author15'])
            print "InspireID", BibAuthorIDHooverTestCase.authors['author15']['inspireID']
            self.assertEquals(inspireID, BibAuthorIDHooverTestCase.authors['author15']['inspireID'])



TEST_SUITE = make_test_suite(OneAuthorOnePaperHooverTestCase, OneAuthorManyPapersHooverTestCase, ManyAuthorsHooverTestCase)
#TEST_SUITE = make_test_suite(HepnamesHooverTestCase)
#TEST_SUITE = make_test_suite(OneAuthorOnePaperHooverTestCase)


if __name__ == "__main__":
    run_test_suite(TEST_SUITE, warn_user=False)
    BibAuthorIDHooverTestCase.tearDownClass()

