from invenio.testutils import make_test_suite
from invenio.testutils import run_test_suite

from invenio.bibauthorid_testutils import *

from unittest import *

from invenio.bibtask import setup_loggers
from invenio.bibtask import task_set_task_param
from invenio.bibupload_regression_tests import wipe_out_record_from_all_tables
from invenio.dbquery import run_sql
from invenio.bibauthorid_rabbit import rabbit
import invenio.bibauthorid_rabbit
from invenio.bibauthorid_hoover import hoover
import invenio.bibauthorid_hoover
from invenio.bibauthorid_dbinterface import get_inspire_id_of_author
from invenio.bibauthorid_dbinterface import _delete_from_aidpersonidpapers_where
from invenio.bibauthorid_dbinterface import get_papers_of_author
from invenio.search_engine import get_record


class BibAuthorIDHooverTestCase(TestCase):

    @classmethod
    def setUpClass(self):

        '''
        Setting up the regression test for hoover.
        '''

        self.verbose = 0
        self.logger = setup_loggers()
        self.logger.info('Setting up regression tests...')
        task_set_task_param('verbose', self.verbose)

        self.authors = { 'author1': {
                                        'name': 'author1111a author1111b',
                                        'inspireID': 'INSPIRE-FAKE_ID1'},
                         'author2': {
                                        'name': 'author2222a author2222b',
                                        'inspireID': 'INSPIRE-FAKE_ID2'},
                         'author3': {
                                        'name': 'author3333a author3333b',
                                        'inspireID': 'INSPIRE-FAKE_ID3'},
                         'author4': {
                                        'name': 'author4444a author4444b',
                                        'inspireID': 'INSPIRE-FAKE_ID4'},
                         'author5': {
                                        'name': 'author5555a author5555b',
                                        'inspireID': 'INSPIRE-FAKE_ID5'},
                         'author6': {
                                        'name': 'author6666a author6666b',
                                        'inspireID': 'INSPIRE-FAKE_ID6'},
                         'author7': {
                                        'name': 'author7777a author7777b',
                                        'inspireID': 'INSPIRE-FAKE_ID7'},
                         'author8': {
                                        'name': 'author8888a author8888b',
                                        'inspireID': 'INSPIRE-FAKE_ID8'},
                         'author9': {
                                        'name': 'author9999a author9999b',
                                        'inspireID': 'INSPIRE-FAKE_ID9'},
                         'author10': {
                                        'name': 'author10101010a author10101010b',
                                        'inspireID': 'INSPIRE-FAKE_ID10'},
                         'author11': {
                                        'name': 'author11111111a author11111111b',
                                        'inspireID': 'INSPIRE-FAKE_ID11'},
                         'author12': {
                                        'name': 'author12121212a author12121212b',
                                        'inspireID': 'INSPIRE-FAKE_ID12'},
                         'author13': {
                                        'name': 'author13131313a author13131313b',
                                        'inspireID': 'INSPIRE-FAKE_ID13'},
                         'author14': {
                                        'name': 'author14141414a author14141414b',
                                        'inspireID': 'INSPIRE-FAKE_ID14'}
                       }
        self.marc_xmls = dict()
        self.bibrecs = dict()
        self.pids = dict()
        self.bibrefs = dict()

        def set_up_test_hoover_inertia():
            self.marc_xmls['paper1'] = get_new_marc_for_test(self.authors['author1']['name'])
            self.bibrecs['paper1'] = get_bibrec_for_record(self.marc_xmls['paper1'], opt_mode='insert')
            self.marc_xmls['paper1'] = add_001_field(self.marc_xmls['paper1'], self.bibrecs['paper1'])

        def set_up_test_hoover_duplication():
            self.marc_xmls['paper2'] = get_new_marc_for_test(self.authors['author2']['name'], None, \
                                                             ((self.authors['author2']['inspireID'], 'i'),))

            self.bibrecs['paper2'] = get_bibrec_for_record(self.marc_xmls['paper2'], opt_mode='insert')
            self.marc_xmls['paper2'] = add_001_field(self.marc_xmls['paper2'], self.bibrecs['paper2'])

        def set_up_test_hoover_assign_one_inspire_id_from_an_unclaimed_paper():
            self.marc_xmls['paper3'] = get_new_marc_for_test(self.authors['author3']['name'], None, \
                                                             ((self.authors['author3']['inspireID'], 'i'),))

            self.bibrecs['paper3'] = get_bibrec_for_record(self.marc_xmls['paper3'], opt_mode='insert')
            self.marc_xmls['paper3'] = add_001_field(self.marc_xmls['paper3'], self.bibrecs['paper3'])

        def set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper():
            self.marc_xmls['paper4'] = get_new_marc_for_test(self.authors['author4']['name'], None, \
                                                             ((self.authors['author4']['inspireID'], 'i'),))

            self.bibrecs['paper4'] = get_bibrec_for_record(self.marc_xmls['paper4'], opt_mode='insert')
            self.marc_xmls['paper4'] = add_001_field(self.marc_xmls['paper4'], self.bibrecs['paper4'])

        def set_up_test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID():
            self.marc_xmls['paper5'] = get_new_marc_for_test(self.authors['author5']['name'], None, \
                                                             ((self.authors['author5']['inspireID'], 'i'),))

            self.bibrecs['paper5'] = get_bibrec_for_record(self.marc_xmls['paper5'], opt_mode='insert')
            self.marc_xmls['paper5'] = add_001_field(self.marc_xmls['paper5'], self.bibrecs['paper5'])

            self.marc_xmls['paper6'] = get_new_marc_for_test(self.authors['author5']['name'], None, \
                                                             ((self.authors['author6']['inspireID'], 'i'),))

            self.bibrecs['paper6'] = get_bibrec_for_record(self.marc_xmls['paper6'], opt_mode='insert')
            self.marc_xmls['paper6'] = add_001_field(self.marc_xmls['paper6'], self.bibrecs['paper6'])

        def set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID():
            self.marc_xmls['paper7'] = get_new_marc_for_test(self.authors['author7']['name'], None, \
                                                             ((self.authors['author7']['inspireID'], 'i'),))

            self.bibrecs['paper7'] = get_bibrec_for_record(self.marc_xmls['paper7'], opt_mode='insert')
            self.marc_xmls['paper7'] = add_001_field(self.marc_xmls['paper7'], self.bibrecs['paper7'])

            self.marc_xmls['paper8'] = get_new_marc_for_test(self.authors['author7']['name'], None, \
                                                             ((self.authors['author8']['inspireID'], 'i'),))

            self.bibrecs['paper8'] = get_bibrec_for_record(self.marc_xmls['paper8'], opt_mode='insert')
            self.marc_xmls['paper8'] = add_001_field(self.marc_xmls['paper8'], self.bibrecs['paper8'])

        def set_up_test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID():
            self.marc_xmls['paper9'] = get_new_marc_for_test(self.authors['author9']['name'], None, \
                                                             ((self.authors['author2']['inspireID'], 'i'),))

            self.bibrecs['paper9'] = get_bibrec_for_record(self.marc_xmls['paper9'], opt_mode='insert')
            self.marc_xmls['paper9'] = add_001_field(self.marc_xmls['paper9'], self.bibrecs['paper9'])

            self.marc_xmls['paper10'] = get_new_marc_for_test(self.authors['author9']['name'], None, \
                                                             ((self.authors['author10']['inspireID'], 'i'),))

            self.bibrecs['paper10'] = get_bibrec_for_record(self.marc_xmls['paper10'], opt_mode='insert')
            self.marc_xmls['paper10'] = add_001_field(self.marc_xmls['paper10'], self.bibrecs['paper10'])

        def set_up_test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper():
            self.marc_xmls['paper11'] = get_new_marc_for_test(self.authors['author11']['name'], None, \
                                                             ((self.authors['author11']['inspireID'], 'i'),))

            self.bibrecs['paper11'] = get_bibrec_for_record(self.marc_xmls['paper11'], opt_mode='insert')
            self.marc_xmls['paper11'] = add_001_field(self.marc_xmls['paper11'], self.bibrecs['paper11'])

            self.marc_xmls['paper12'] = get_new_marc_for_test(self.authors['author12']['name'], None, \
                                                             ((self.authors['author11']['inspireID'], 'i'),))

            self.bibrecs['paper12'] = get_bibrec_for_record(self.marc_xmls['paper12'], opt_mode='insert')
            self.marc_xmls['paper12'] = add_001_field(self.marc_xmls['paper12'], self.bibrecs['paper12'])

        def set_up_test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper():
            self.marc_xmls['paper13'] = get_new_marc_for_test(self.authors['author13']['name'], None, \
                                                             ((self.authors['author13']['inspireID'], 'i'),))

            self.bibrecs['paper13'] = get_bibrec_for_record(self.marc_xmls['paper13'], opt_mode='insert')
            self.marc_xmls['paper13'] = add_001_field(self.marc_xmls['paper13'], self.bibrecs['paper13'])

            self.marc_xmls['paper14'] = get_new_marc_for_test(self.authors['author14']['name'], None, \
                                                             ((self.authors['author13']['inspireID'], 'i'),))

            self.bibrecs['paper14'] = get_bibrec_for_record(self.marc_xmls['paper14'], opt_mode='insert')
            self.marc_xmls['paper14'] = add_001_field(self.marc_xmls['paper14'], self.bibrecs['paper14'])

        set_up_test_hoover_inertia()
        set_up_test_hoover_duplication()
        set_up_test_hoover_assign_one_inspire_id_from_an_unclaimed_paper()
        set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper()
        set_up_test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID()
        set_up_test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID()
        set_up_test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID()
        set_up_test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper()
        set_up_test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper()

        self.bibrecs_to_clean = [self.bibrecs[key] for key in self.bibrecs]
        rabbit([self.bibrecs[key] for key in self.bibrecs], verbose=False)

        for key in self.authors:
            self.bibrefs[key] = get_bibref_value_for_name(self.authors[key]['name'])
            self.pids[key] = run_sql("select personid from aidPERSONIDPAPERS where bibref_value=%s and bibrec=%s and name=%s", (self.bibrefs[key], self.bibrecs[key.replace('author','paper')], self.authors[key]['name']))[0][0]

        claim_test_paper(self.bibrecs['paper4'])
        claim_test_paper(self.bibrecs['paper7'])
        claim_test_paper(self.bibrecs['paper9'])
        claim_test_paper(self.bibrecs['paper10'])
        claim_test_paper(self.bibrecs['paper11'])
        claim_test_paper(self.bibrecs['paper13'])
        claim_test_paper(self.bibrecs['paper14'])
        hoover(list(set([self.pids[key] for keys in self.pids])))

    @classmethod
    def tearDownClass(self):
        # All records are wiped out for consistency.
        self.clean_up_the_database(self.authors['author1']['inspireID'])
        self.clean_up_the_database(self.authors['author2']['inspireID'])
        self.clean_up_the_database(self.authors['author3']['inspireID'])
        self.clean_up_the_database(self.authors['author4']['inspireID'])
        self.clean_up_the_database(self.authors['author5']['inspireID'])
        self.clean_up_the_database(self.authors['author6']['inspireID'])
        self.clean_up_the_database(self.authors['author7']['inspireID'])
        self.clean_up_the_database(self.authors['author8']['inspireID'])
        self.clean_up_the_database(self.authors['author9']['inspireID'])
        self.clean_up_the_database(self.authors['author10']['inspireID'])
        self.clean_up_the_database(self.authors['author11']['inspireID'])
        self.clean_up_the_database(self.authors['author12']['inspireID'])
        
        for key in self.pids:
            _delete_from_aidpersonidpapers_where(self.pids[key])

        for bibrec in self.bibrecs_to_clean:
            wipe_out_record_from_all_tables(bibrec)
            clean_authors_tables(bibrec)

    def clean_up_the_database(self, inspireID):
        if inspireID:
            run_sql("delete from aidPERSONIDDATA where data=%s", (inspireID,))


class OneAuthorOnePaperHooverTestCase(BibAuthorIDHooverTestCase):

    def setUp(self):
        super(OneAuthorOnePaperHooverTestCase, self).setUp()

    def tearDown(self):
        super(OneAuthorOnePaperHooverTestCase, self).tearDown()

    def test_hoover_one_author_one_paper(self):

        def test_hoover_inertia():
            '''If nothing should change then nothing changes'''

            inspireID = get_inspire_id_of_author(self.pids['author1'])
            self.assertEquals(inspireID, tuple())

        def test_hoover_for_duplication():
            '''No duplicated information in the database'''

            #author_papers = get_papers_of_author(self.pids['author1'])
            #inspire_list_after = run_sql("select * from aidPERSONIDDATA where tag='extid:INSPIREID' and data=%s",(inspireID_after,))
            inspireID = get_inspire_id_of_author(self.pids['author2'])
            self.assertEquals(inspireID, tuple())
            #self.assertEquals(author_papers_before, author_papers_after)

        def test_hoover_assign_one_inspire_id_from_an_unclaimed_paper():
            '''
            Preconditions:
                *This is the only paper that the author has
                *No other author has a claim on the paper
            Postconditions:
                *connect author with inspireID taken from the unclaimed paper
            '''

            inspireID = get_inspire_id_of_author(self.pids['author3'])
            self.assertEquals(inspireID, self.authors['author3']['inspireID'])

        def test_hoover_assign_one_inspire_id_from_a_claimed_paper():
            '''
            Preconditions:
                *This is the only paper that the author has
                *No other author has a claim on the paper
            Postconditions:
                *connect author with inspireID taken from the claimed paper
            '''

            inspireID = get_inspire_id_of_author(self.pids['author4'])
            self.assertEquals(inspireID, self.authors['author4']['inspireID'])

        test_hoover_inertia()
        test_hoover_for_duplication()
        test_hoover_assign_one_inspire_id_from_an_unclaimed_paper()
        test_hoover_assign_one_inspire_id_from_a_claimed_paper()

class OneAuthorManyPapersHooverTestCase(BibAuthorIDHooverTestCase):

    def setUp(self):
        super(OneAuthorManyPapersHooverTestCase, self).setUp()

    def tearDown(self):
        super(OneAuthorManyPapersHooverTestCase, self).tearDown()

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

            inspireID = get_inspire_id_of_author(self.pids['author5'])
            self.assertEquals(inspireID, set())

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

            inspireID = get_inspire_id_of_author(self.pids['author7'])
            self.assertEquals(inspireID, self.authors['author7']['inspireID'])

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

            inspireID = get_inspire_id_of_author(self.pids['author9'])
            self.assertEquals(inspireID, set())

        test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID()
        test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID()
        test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID()

class ManyAuthorsHooverTestCase(BibAuthorIDHooverTestCase):

    def setUp(self):
        super(ManyAuthorsHooverTestCase, self).setUp()

    def tearDown(self):
        super(ManyAuthorsHooverTestCase, self).tearDown()

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

            first_author_papers = get_papers_of_author(self.pids['author11'])
            second_author_papers = get_papers_of_author(self.pids['author12'])

            inspireID1 = get_inspire_id_of_author(self.pids['author11'])
            self.assertEquals(inspireID1, self.authors['author11']['inspireID'])
            inspireID2 = get_inspire_id_of_author(self.pids['author12'])
            self.assertEquals(inspireID2, set())

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


            first_author_papers = get_papers_of_author(self.pids['author13'])
            second_author_papers = get_papers_of_author(self.pids['author14'])

            inspireID1 = get_inspire_id_of_author(self.pids['author13'])
            self.assertEquals(inspireID1, self.authors['author13']['inspireID'])
            inspireID2 = get_inspire_id_of_author(self.pids['author14'])
            self.assertEquals(inspireID2, set())

            self.assertEquals(len(first_author_papers), 1)
            self.assertEquals(len(second_author_papers), 1)

        test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper()
        test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper()

TEST_SUITE = make_test_suite(OneAuthorOnePaperHooverTestCase, OneAuthorManyPapersHooverTestCase, ManyAuthorsHooverTestCase)
#TEST_SUITE = make_test_suite(ManyAuthorsHooverTestCase)
#TEST_SUITE = make_test_suite(OneAuthorOnePaperHooverTestCase)


if __name__ == "__main__":
    run_test_suite(TEST_SUITE, warn_user=False)
