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

    def setUp(self):

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
                                        'inspireID': 'INSPIRE-FAKE_ID12'}
                       }
        self.marc_xmls = dict()




    def tearDown(self):
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
        for bibrec in self.bibrecs_to_clean:
            wipe_out_record_from_all_tables(bibrec)
            clean_authors_tables(bibrec)

    def clean_up_the_database(self, inspireID):
        if inspireID:
            run_sql("delete from aidPERSONIDDATA where data=%s", (inspireID,))


class OneAuthorOnePaperHooverTestCase(BibAuthorIDHooverTestCase):

    def setUp(self):
        super(OneAuthorOnePaperHooverTestCase, self).setUp()
        self.unclaimed_marcxml_record = get_new_marc_for_test(self.authors['author1']['name'], None, \
                                                         ((self.authors['author1']['inspireID'], 'i'),))

        self.claimed_marcxml_record = get_new_marc_for_test(self.authors['author1']['name'], None, \
                                                         ((self.authors['author1']['inspireID'], 'i'),))

        self.insignificant_marcxml_record = get_new_marc_for_test(self.authors['author2']['name'])
                                                         
        self.unclaimed_bibrec = get_bibrec_for_record(self.unclaimed_marcxml_record, opt_mode='insert')
        self.unclaimed_marcxml_record = add_001_field(self.unclaimed_marcxml_record, self.unclaimed_bibrec)

        self.claimed_bibrec = get_bibrec_for_record(self.claimed_marcxml_record, opt_mode='insert')
        self.claimed_marcxml_record = add_001_field(self.claimed_marcxml_record, self.claimed_bibrec)

        self.insignificant_bibrec = get_bibrec_for_record(self.insignificant_marcxml_record, opt_mode='insert')
        self.insignificant_marcxml_record = add_001_field(self.insignificant_marcxml_record, self.insignificant_bibrec)

        self.bibrecs_to_clean = [self.unclaimed_bibrec, self.claimed_bibrec, self.insignificant_bibrec]

        rabbit([self.unclaimed_bibrec, self.claimed_bibrec, self.insignificant_bibrec], verbose=False)

        self.current_bibref_value = get_bibref_value_for_name(self.authors['author1']['name']) #saved for following tests
        self.insignificant_bibref_value = get_bibref_value_for_name(self.authors['author2']['name']) #saved for following tests

        self.pid = run_sql("select personid from aidPERSONIDPAPERS where bibref_value=%s and bibrec=%s and name=%s", (self.current_bibref_value, self.unclaimed_bibrec, self.authors['author1']['name']))[0][0]
        self.insignificant_pid = run_sql("select personid from aidPERSONIDPAPERS where bibref_value=%s and bibrec=%s and name=%s", (self.insignificant_bibref_value, self.insignificant_bibrec, self.authors['author2']['name']))[0][0]

    def tearDown(self):
        _delete_from_aidpersonidpapers_where(self.pid)
        _delete_from_aidpersonidpapers_where(self.insignificant_pid)
        super(OneAuthorOnePaperHooverTestCase, self).tearDown()

    def test_hoover_one_author_one_paper(self):

        def test_hoover_inertia():
            '''If nothing should change then nothing changes'''

            hoover([self.insignificant_pid])
            inspireID = get_inspire_id_of_author(self.insignificant_pid)
            self.assertEquals(inspireID, tuple())

        def test_hoover_for_duplication():
            '''No duplicated information in the database'''

            hoover([self.pid])
            author_papers_before = get_papers_of_author(self.pid)
            inspireID_before = get_inspire_id_of_author(self.pid)
            inspire_list_before = run_sql("select * from aidPERSONIDDATA where tag='extid:INSPIREID' and data=%s",(inspireID_before,))
            hoover([self.pid])
            inspireID_after = get_inspire_id_of_author(self.pid)
            author_papers_after = get_papers_of_author(self.pid)
            inspire_list_after = run_sql("select * from aidPERSONIDDATA where tag='extid:INSPIREID' and data=%s",(inspireID_after,))
            self.assertEquals(author_papers_before, author_papers_after)
            self.assertEquals(inspire_list_before, inspire_list_after)
            self.assertEquals(inspireID_before, inspireID_after)
            self.clean_up_the_database(inspireID_after)

        def test_hoover_assign_one_inspire_id_from_an_unclaimed_paper():
            '''
            Preconditions:
                *This is the only paper that the author has
                *No other author has a claim on the paper
            Postconditions:
                *connect author with inspireID taken from the unclaimed paper
            '''

            inspireID_before = get_inspire_id_of_author(self.pid)
            hoover([self.pid])
            inspireID_after = get_inspire_id_of_author(self.pid)
            self.assertEquals(inspireID_after, 'INSPIRE-FAKE_ID1')
            self.clean_up_the_database(inspireID_after)

        def test_hoover_assign_one_inspire_id_from_a_claimed_paper():
            '''
            Preconditions:
                *This is the only paper that the author has
                *No other author has a claim on the paper
            Postconditions:
                *connect author with inspireID taken from the claimed paper
            '''

            claim_test_paper(self.claimed_bibrec)
            inspireID_before = get_inspire_id_of_author(self.pid)
            hoover([self.pid])
            inspireID_after = get_inspire_id_of_author(self.pid)
            self.assertEquals(inspireID_after, 'INSPIRE-FAKE_ID1')
            self.clean_up_the_database(inspireID_after)

        test_hoover_inertia()
        test_hoover_for_duplication()
        test_hoover_assign_one_inspire_id_from_an_unclaimed_paper()
        test_hoover_assign_one_inspire_id_from_a_claimed_paper()

class OneAuthorManyPapersHooverTestCase(BibAuthorIDHooverTestCase):

    def setUp(self):
        super(OneAuthorManyPapersHooverTestCase, self).setUp()
        self.first_marcxml_record = get_new_marc_for_test(self.authors['author1']['name'], None, \
                                                         ((self.authors['author1']['inspireID'], 'i'),))

        self.second_marcxml_record = get_new_marc_for_test(self.authors['author1']['name'], None, \
                                                          ((self.authors['author2']['inspireID'], 'i'),))

        self.first_bibrec = get_bibrec_for_record(self.first_marcxml_record, opt_mode='insert')
        self.first_marcxml_record = add_001_field(self.first_marcxml_record, self.first_bibrec)

        self.second_bibrec = get_bibrec_for_record(self.second_marcxml_record, opt_mode='insert')
        self.second_marcxml_record = add_001_field(self.second_marcxml_record, self.second_bibrec)

        self.bibrecs_to_clean = [self.first_bibrec, self.second_bibrec]

        rabbit([self.first_bibrec, self.second_bibrec], verbose=False)

        self.current_bibref_value = get_bibref_value_for_name(self.authors['author1']['name']) #saved for following tests
        self.pid = run_sql("select personid from aidPERSONIDPAPERS where bibref_value=%s and bibrec=%s and name=%s", (self.current_bibref_value, self.first_bibrec, self.authors['author1']['name']))[0][0]

    def tearDown(self):
        _delete_from_aidpersonidpapers_where(self.pid)
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

            inspireID_before = get_inspire_id_of_author(self.pid)
            print "inspireID_before:", inspireID_before
            hoover([self.pid])
            inspireID_after = get_inspire_id_of_author(self.pid)
            print "inspireID_after:", inspireID_after
            self.assertEquals(inspireID_after, tuple())
            self.clean_up_the_database(inspireID_after)

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

            claim_test_paper(self.first_bibrec)
            inspireID_before = get_inspire_id_of_author(self.pid)
            print "inspireID_before:", inspireID_before
            hoover([self.pid])
            inspireID_after = get_inspire_id_of_author(self.pid)
            print "inspireID_after:", inspireID_after
            self.assertEquals(inspireID_after, 'INSPIRE-FAKE_ID1')
            self.clean_up_the_database(inspireID_after)

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

            claim_test_paper(self.second_bibrec)
            inspireID_before = get_inspire_id_of_author(self.pid)
            print "inspireID_before:", inspireID_before
            hoover([self.pid])
            inspireID_after = get_inspire_id_of_author(self.pid)
            print "inspireID_after:", inspireID_after
            self.assertEquals(inspireID_after, tuple())
            self.clean_up_the_database(inspireID_after)

        test_hoover_assign_one_inspire_id_from_unclaimed_papers_with_different_inspireID()
        test_hoover_assign_one_inspire_id_from_a_claimed_paper_and_unclaimed_paper_with_different_inspireID()
        test_hoover_assign_one_inspire_id_from_claimed_papers_with_different_inspireID()

class ManyAuthorsHooverTestCase(BibAuthorIDHooverTestCase):

    def setUp(self):
        super(ManyAuthorsHooverTestCase, self).setUp()
        self.first_marcxml_record = get_new_marc_for_test(self.authors['author1']['name'], None, \
                                                             ((self.authors['author1']['inspireID'], 'i'),))
        self.second_marcxml_record = get_new_marc_for_test(self.authors['author2']['name'], None, \
                                                             ((self.authors['author1']['inspireID'], 'i'),))
        self.first_bibrec = get_bibrec_for_record(self.first_marcxml_record, opt_mode='insert')
        self.first_marcxml_record = add_001_field(self.first_marcxml_record, self.first_bibrec)

        self.second_bibrec = get_bibrec_for_record(self.second_marcxml_record, opt_mode='insert')
        self.second_marcxml_record = add_001_field(self.second_marcxml_record, self.second_bibrec)

        self.bibrecs_to_clean = [self.first_bibrec, self.second_bibrec]

        rabbit([self.first_bibrec, self.second_bibrec], verbose=False)
        self.current_bibref_value = get_bibref_value_for_name(self.authors['author1']['name']) #saved for following tests
        self.pid_first_author = run_sql("select personid from aidPERSONIDPAPERS where bibref_value=%s and bibrec=%s and name=%s",\
                                   (self.current_bibref_value, self.first_bibrec, self.authors['author1']['name']))[0][0]
        print "First pid", self.pid_first_author
        print "First bibref", self.current_bibref_value
        self.current_bibref_value = get_bibref_value_for_name(self.authors['author2']['name']) #saved for following tests
        self.pid_second_author = run_sql("select personid from aidPERSONIDPAPERS where bibref_value=%s and bibrec=%s and name=%s",\
                                   (self.current_bibref_value, self.second_bibrec, self.authors['author2']['name']))[0][0]
        print "Second pid", self.pid_second_author
        print "Second bibref", self.current_bibref_value
        print "first marc", self.first_marcxml_record
        print "second marc", self.second_marcxml_record
        print "first record", get_record(self.first_bibrec) 
        print "second record", get_record(self.second_bibrec)
        print "sql: ", run_sql('select * from aidPERSONIDPAPERS where name LIKE "author%"')



    def tearDown(self):
        _delete_from_aidpersonidpapers_where(self.pid_first_author)
        _delete_from_aidpersonidpapers_where(self.pid_second_author)
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

            claim_test_paper(self.first_bibrec)
            first_author_papers_before = get_papers_of_author(self.pid_first_author)
            second_author_papers_before = get_papers_of_author(self.pid_second_author)
            inspireID_before = get_inspire_id_of_author(self.pid_first_author)

            print "inspireID_before:", inspireID_before
            print "first_author_papers_before:", first_author_papers_before
            print "second_author_papers_before:", second_author_papers_before

            hoover([self.pid_second_author, self.pid_first_author])

            first_author_papers_after = get_papers_of_author(self.pid_first_author)
            second_author_papers_after = get_papers_of_author(self.pid_second_author)

            first_author_inspireID_after = get_inspire_id_of_author(self.pid_first_author)
            second_author_inspireID_after = get_inspire_id_of_author(self.pid_second_author)

            print "first_author_inspireID_after:", first_author_inspireID_after
            print "second_author_inspireID_after:", second_author_inspireID_after
            print "first_author_papers_after:", first_author_papers_after
            print "second_author_papers_after:", second_author_papers_after

            self.assertEquals(first_author_inspireID_after, 'INSPIRE-FAKE_ID1')
            self.assertEquals(second_author_inspireID_after, tuple())

            first_author_papers_before = set(x[1:4] for x in first_author_papers_before)
            second_author_papers_before = set(x[1:4] for x in second_author_papers_before)
            first_author_papers_after = set(x[1:4] for x in first_author_papers_after)
            second_author_papers_after = set(x[1:4] for x in second_author_papers_after)
            self.assertEquals(first_author_papers_after, first_author_papers_before.union(second_author_papers_before))
            self.assertEquals(second_author_papers_after, set())
            self.clean_up_the_database(first_author_inspireID_after)
            self.clean_up_the_database(second_author_inspireID_after)

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

            self.second_marcxml_record = get_modified_marc_for_test(self.second_marcxml_record)
            self.second_marcxml_record = get_bibrec_for_record(self.second_marcxml_record,
                                                     opt_mode='replace')
            rabbit([self.second_bibrec], verbose=False)
            claim_test_paper(self.second_bibrec)

            first_author_papers_before = get_papers_of_author(self.pid_first_author)
            second_author_papers_before = get_papers_of_author(self.pid_second_author)
            inspireID_before = get_inspire_id_of_author(self.pid_first_author)

            print "inspireID_before:", inspireID_before
            print "first_author_papers_before:", first_author_papers_before
            print "second_author_papers_before:", second_author_papers_before

            hoover([self.pid_second_author, self.pid_first_author])

            first_author_papers_after = get_papers_of_author(self.pid_first_author)
            second_author_papers_after = get_papers_of_author(self.pid_second_author)

            first_author_inspireID_after = get_inspire_id_of_author(self.pid_first_author)
            second_author_inspireID_after = get_inspire_id_of_author(self.pid_second_author)

            print "first_author_inspireID_after:", first_author_inspireID_after
            print "second_author_inspireID_after:", second_author_inspireID_after
            print "first_author_papers_after:", first_author_papers_after
            print "second_author_papers_after:", second_author_papers_after

            self.assertEquals(first_author_inspireID_after, 'INSPIRE-FAKE_ID1')
            self.assertEquals(second_author_inspireID_after, tuple())

            first_author_papers_before = set(x[1:4] for x in first_author_papers_before)
            second_author_papers_before = set(x[1:4] for x in second_author_papers_before)
            first_author_papers_after = set(x[1:4] for x in first_author_papers_after)
            second_author_papers_after = set(x[1:4] for x in second_author_papers_after)
            self.assertEquals(first_author_papers_after, first_author_papers_before)
            self.assertEquals(second_author_papers_after, second_author_papers_before)
            self.clean_up_the_database(first_author_inspireID_after)
            self.clean_up_the_database(second_author_inspireID_after)

        test_hoover_vacuum_an_unclaimed_paper_with_an_inspire_id_from_a_claimed_paper()
        test_hoover_vacuum_a_claimed_paper_with_an_inspire_id_from_a_claimed_paper()

TEST_SUITE = make_test_suite(OneAuthorOnePaperHooverTestCase, OneAuthorManyPapersHooverTestCase, ManyAuthorsHooverTestCase)
#TEST_SUITE = make_test_suite(ManyAuthorsHooverTestCase)
#TEST_SUITE = make_test_suite(OneAuthorOnePaperHooverTestCase)


if __name__ == "__main__":
    run_test_suite(TEST_SUITE, warn_user=False)
