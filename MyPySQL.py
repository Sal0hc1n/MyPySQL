import MySQLdb
from operator import itemgetter

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class DB(object):
    """docstring for DB."""
    def __init__(self, address, user, password, name):
        self.address = address
        self.user = user
        self.password = password
        self.name = name

        self.db = MySQLdb.connect(address, user, password, name)
        self.cursor = self.db.cursor()

    def __dict__(self):
        data = self.cursor.fetchone()
        if data == None:
            return None
        desc = self.cursor.description

        dictionary = {}

        for (name, value) in zip(desc, data):
            dictionary[name[0]] = value

        return dictionary

    def close(self):
        self.db.close()

    def execute(self, query):
        self.cursor.execute(query)

    def colors(self, arg = 'ENDC'):
        switcher = {
            'HEADER'    : '\033[95m',
            'OKBLUE'    : '\033[94m',
            'OKGREEN'   : '\033[92m',
            'WARNING'   : '\033[93m',
            'FAIL'      : '\033[91m',
            'ENDC'      : '\033[0m',
            'BOLD'      : '\033[1m',
            'UNDERLINE' : '\033[4m'
        }

        return switcher.get(arg, '\033[0m')


    def check(self, query, ok = None, err = None):
        '''
        check with messages
        '''
        try:
           self.execute(query)
           self.db.commit()
           if ok is not None:
               print ok
           else:
               print 'committed'

           return True
        except:
           self.db.rollback()

           if err is not None:
               print err
           else:
                print 'Rollback'

           return False

    def checkc(self, query, ok = None, err = None, okColor = 'OKGREEN', errColor = 'FAIL'):
        '''
        checkc colored
        '''
        try:
           self.execute(query)
           self.db.commit()

           if ok is not None:
               print self.colors(okColor) + ok + self.colors()
           else:
               print self.colors(okColor) + 'committed' + self.colors()

           return True

        except:
           self.db.rollback()

           if err is not None:
               print self.colors(errColor) + err + self.colors()
           else:
               print self.colors(errColor) + 'Rollback' + self.colors()


           return False


    def log(self, log):
        pass

    def fetchOneAssoc(self, cursor):
        data = cursor.fetchone()
        if data is None:
            return None

        desc = cursor.description

        dictionary = {}

        for (name, value) in zip(desc, data):
            dictionary[name[0]] = value

        return dictionary

    def format_as_table(self, data,
                        keys,
                        header=None,
                        sort_by_key=None,
                        sort_order_reverse=False):
        """Takes a list of dictionaries, formats the data, and returns
        the formatted data as a text table.

        Required Parameters:
            data - Data to process (list of dictionaries). (Type: List)
            keys - List of keys in the dictionary. (Type: List)

        Optional Parameters:
            header - The table header. (Type: List)
            sort_by_key - The key to sort by. (Type: String)
            sort_order_reverse - Default sort order is ascending, if
                True sort order will change to descending. (Type: Boolean)
        """
        # Sort the data if a sort key is specified (default sort order
        # is ascending)
        if sort_by_key:
            data = sorted(data,
                          key=itemgetter(sort_by_key),
                          reverse=sort_order_reverse)

        # If header is not empty, add header to data
        if header:
            # Get the length of each header and create a divider based
            # on that length
            header_divider = []
            for name in header:
                header_divider.append('-' * len(name))

            # Create a list of dictionary from the keys and the header and
            # insert it at the beginning of the list. Do the same for the
            # divider and insert below the header.
            header_divider = dict(zip(keys, header_divider))
            data.insert(0, header_divider)
            header = dict(zip(keys, header))
            data.insert(0, header)

        column_widths = []
        for key in keys:
            column_widths.append(max(len(str(column[key])) for column in data))

        # Create a tuple pair of key and the associated column width for it
        key_width_pair = zip(keys, column_widths)

        format = ('%-*s ' * len(keys)).strip() + '\n'
        formatted_data = ''
        for element in data:
            data_to_format = []
            # Create a tuple that will be used for the formatting in
            # width, value format
            for pair in key_width_pair:
                data_to_format.append(pair[1])
                data_to_format.append(element[pair[0]])
            formatted_data += format % tuple(data_to_format)
        return formatted_data

    def printTB(self, cursor = None, header = None, sort_by_key = None, sort_order_reverse = False):

        li = []

        if cursor is None:
            fetch = self.fetchOneAssoc(self.cursor)
            if fetch is None:
                print 'None fetch'
        else:
            fetch = self.fetchOneAssoc(cursor)

        print bcolors.OKBLUE + '|LOG| collecting...' + bcolors.ENDC

        while fetch is not None:
            li.append( fetch )
            fetch = self.fetchOneAssoc(self.cursor)

        print  bcolors.OKBLUE + '|LOG| formatting...'  + bcolors.ENDC

        if header is None:
            print bcolors.OKBLUE + '|LOG| autocollectiong header...' + bcolors.ENDC

            print bcolors.OKBLUE + '|LOG| header founded: ',list(li[0].keys()), '\n' + bcolors.ENDC

            print self.format_as_table(li, list(li[0].keys()), list(li[0].keys()), sort_by_key, sort_order_reverse)
        else:
            print bcolors.OKBLUE + '|LOG| header found!' + bcolors.ENDC

            print self.format_as_table(li, list(li[0].keys()), header, sort_by_key, sort_order_reverse)