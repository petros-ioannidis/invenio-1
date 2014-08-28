from invenio.bibauthorid_dbinterface import get_canonical_name_of_author, get_name_by_bibref
class HooverException(Exception):

    def __init__(self):
        pass

    def get_message_body(self):
        raise NotImplementedError(self.__repr__())

    def get_message_subject(self):
        raise NotImplementedError(self.__repr__())
class InconsistentIdentifiersException(HooverException):
    """Exception Class for the case of different reliable identifiers in the
    database
    """
    def __init__(self, message, pid, identifier_type, ids_list):
        Exception.__init__(self, message)
        self.pid = pid
        self.identifier_type = identifier_type
        self.ids_list = ids_list

    def get_message_subject(self):
        return '[Hoover] Author found with multiple identifiers of the same kind'

    def get_message_body(self):
        msg = ["Found multiple different %s identifiers (%s) on profile: " % (self.identifier_type, ','.join(self.ids_list))]
        msg.append("http://inspirehep.net/author/profile/%s" % get_canonical_name_of_author(self.pid) )
        msg.append(self.message)
        return '\n'.join(msg)

class DuplicatePaperException(HooverException):
    """Base class for duplicated papers conflicts"""

    def __init__(self, message, pid, signature, present_signatures):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        pid -- the pid of the author that caused the exception
        signature -- the signature that raise the exception
        """
        Exception.__init__(self, message)
        self.pid = pid
        self.signature = signature
        self.present_signatures = present_signatures

class DuplicateClaimedPaperException(DuplicatePaperException):
    """Class for duplicated papers conflicts when one of them is claimed"""
    def get_message_subject(self):
        return '[Hoover] Wrong signature claimed to profile'

    def get_message_body(self):
        msg = ['Found wrong signature claimed to profile ']
        try:
            cname = get_canonical_name_of_author(self.pid)[0]
        except IndexError:
            cname = self.pid

        msg.append("http://inspirehep.net/author/profile/%s" % cname)
        #TODO: add to exception information about which ID is requiring the move
        sig_name = get_name_by_bibref(self.signature[0:2])
        p_sigs = [(x,get_name_by_bibref(x[0:2])) for x in self.present_signatures]

        p_sig_strings = ",".join( '%s (%s on record %s)' % (x[0],x[1],x[0][2]) for x in p_sigs)

        msg.append("want to move %s (%s on record %s) to this profile but [%s] are already present and claimed" %
                    (self.signature, sig_name, self.signature[2], p_sig_strings))
        msg.append(self.message)
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
        msg.append('Something went wrong while trying to read the %s identifier' % self.identifier_type)
        msg.append(self.message)
        return '\n'.join(msg)

class NoCanonicalNameException(HooverException):
    """Base class for no canonical name found for a pid"""

    def __init__(self, message, pid):
        """Set up the exception class

        arguments:
        message -- the message to be displayed when the exceptions is raised
        pid -- the pid of the author that lacks a canonical name
        """
        Exception.__init__(self, message)
        self.pid = pid

class ConflictingIdsOnRecordException(HooverException):
    def __init__(self, message, pid, identifier_type, ids_list, record):
        Exception.__init__(self, message)
        self.pid = pid
        self.identifier_type = identifier_type
        self.ids_list = ids_list
        self.record = record

    def get_message_subject(self):
        return '[Hoover] Signature on record holds more then one identifiers of the same kind'

    def get_message_body(self):
        msg = ['Signature on record holds more then one identifiers of the same kind']
        msg.append("http://inspirehep.net/record/%s" % self.record)
        msg.append("The following ids are associated to the same name: %s" % ', '.join(self.ids_list))
        msg.append(self.message)
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
        msg.append('Those profiles are sharing the same %s identifier!' % self.identifier_type)
        msg.append(self.message)
        return '\n'.join(msg)

class MultipleIdsOnSingleAuthorException(HooverException):
    """Base class for multiple ids on a single author"""

    def __init__(self, message, pid, identifier_type, ids):
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
        msg = ['Found profile with multiple conflicting user-verified identifiers: ']
        msg += ['http://inspirehep.net/author/profile/%s' % self.pid]
        msg.append('This profile has all this %s identifiers:' % self.identifier_type)
        msg.append(', '.join(str(x) for x in self.ids))
        msg.append('Each profile should have only one identifier of each kind.')
        msg.append(self.message)
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
        msg.append(self.message)
        return '\n'.join(msg)

