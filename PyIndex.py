from PyXapian import PyXapian
import MySQLdb
import MySQLdb.cursors
import xapian

from django.conf import settings

new_ids = []
old_ids = []
class PyIndex(object):
    def __init__(self, dbname):
        self.dbname = dbname
        self._pyxapian = PyXapian(self.dbname)
        self._field_id_name = self._pyxapian._scheme.getFieldID().name

    def dataFromMysql(self):
        m = settings.XAPIAN_MYSQL[self.dbname]
        conn = MySQLdb.connect(host=m['host'], user=m['user'], passwd=m['passwd'], db=m['db'], charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
        cur = conn.cursor()
        sql = 'select * from %s' % m['table']
        cur.execute(sql)
        result = cur.fetchall()
        for i,row in enumerate(result):
            print i
            self._pyxapian.replace_row(row)
            new_ids.append(long(row.get(self._field_id_name)))
        conn.close()

    def searchAll(self):
        self._pyxapian.set_limit(0, 1000000)
        return self._pyxapian.search()

    def del_old_doc(self):
        try:
            docs = self.searchAll()
        except:
            docs = []
        for doc in docs:
            old_ids.append(long(doc.get(self._field_id_name)))
        diff = set(old_ids) - set(new_ids)
        for i in diff:
            self._pyxapian.del_doc(str(i))


    def begin(self):
        self._pyxapian.xapian_init_writable()
        self._pyxapian.begin_transaction()

    def end(self):
        self._pyxapian.commit_transaction()

    def rebuild(self):
        new_ids = []
        old_ids = []
        self.begin()
        self.dataFromMysql()
        self.del_old_doc()
        self.end()

def run(name):
    x = PyIndex(name)
    x.rebuild()

def update(name,data):
    x = PyIndex(name)
    x.begin()
    data['id'] = data['pk']
    x._pyxapian.replace_row(data)
    x.end()

def delete(name,i):
    x = PyIndex(name)
    x.begin()
    x._pyxapian.del_doc(str(i))
    x.end()
