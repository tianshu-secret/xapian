import xapian
import os
import re

from django.conf import settings
from PyFieldScheme import PyFieldScheme

DOCUMENT_ID_TERM_PREFIX = u'Q'
DOCUMENT_CUSTOM_TERM_PREFIX = u'X'

#####################CLASS########################################
class PyXapian(object):
    def __init__(self,name):
        self._sort = {}
        self._facets = {}
        self._range = {}

        self._offset = 0
        self._count = 0

        self._query = xapian.Query('')

        self._xapian_db_path = settings.XAPIAN_DB_PATH + 'data/' + name
        self.mkdir(self._xapian_db_path)

        app = settings.XAPIAN_PROJECT[name]
        self._scheme = PyFieldScheme()
        for (name, data) in app.items():
            self._scheme.addField(name, data)
            
    def mkdir(self,path):
        path = path.strip().rstrip()
        if not os.path.exists(self._xapian_db_path):
            os.makedirs(path)

    def xapian_init_readonly(self):
        self._xapian_read_db = xapian.Database(self._xapian_db_path)
        self._xapian_stemmer = xapian.Stem("english")
        self._xapian_enquire = xapian.Enquire(self._xapian_read_db)

    def xapian_init_writable(self):
        self._xapian_write_db = xapian.WritableDatabase(self._xapian_db_path, xapian.DB_CREATE_OR_OPEN)
        self._xapian_indexer = xapian.TermGenerator()
        self._xapian_stemmer = xapian.Stem("english")
        self._xapian_indexer.set_stemmer(self._xapian_stemmer)

    def begin_transaction(self):
        return self._xapian_write_db.begin_transaction()

    def commit_transaction(self):
       return self._xapian_write_db.commit_transaction()

    def replace_row(self, row = {}):
        document = xapian.Document()
        self._xapian_indexer.set_document(document)
        doc_id = DOCUMENT_ID_TERM_PREFIX + str(row.get(self._scheme.getFieldID().name))
        document.add_term(doc_id)
        for (name, field) in self._scheme.getAllFields().items():
            value = field.convert(row.get(name))
            if value is None:
                continue
            document.add_value(field.vno, value)
            if field.hasIndex():
                t = u''
                for s in unicode(row.get(name)):
                    t += s + u' '
                t = t[:-1]
                self._xapian_indexer.index_text( t, field.weight, DOCUMENT_CUSTOM_TERM_PREFIX + field.name )
        self._xapian_write_db.replace_document(str(doc_id), document)

    def del_doc(self, doc_id):
        doc_id = DOCUMENT_ID_TERM_PREFIX + doc_id
        self._xapian_write_db.delete_document(doc_id)
        return True

#####################INDEX########################################

    def indexParser(self, field, query):
        qstring = ''
        query = re.compile(ur"[^\u4e00-\u9fa50-9a-zA-Z]").sub('', query)
        key = DOCUMENT_CUSTOM_TERM_PREFIX + field.name
        for i in query:
            qstring += key + u':' + i + u' ADJ/1 '
        qstring = qstring[:-7]
        return qstring

#####################GET########################################

    def get_facets(self, facet):
        facets = {}
        spy = self._facets.get(facet)
        field = self._scheme.getOneFields(facet)
        for i in spy.values():
            facets[field.deconvert(i.term)] = i.termfreq
        return facets

    def get_qp(self,field):
        qp = xapian.QueryParser()
        qp.set_stemmer(self._xapian_stemmer)
        qp.set_database(self._xapian_read_db)
        qp.set_stemming_strategy(xapian.QueryParser.STEM_SOME)
        prefix = DOCUMENT_CUSTOM_TERM_PREFIX + field.name
        qp.add_prefix(prefix, prefix)
        return qp

    def get_choice(self,choice):
        if 'filter' == choice:
            return xapian.Query.OP_FILTER
        if 'or' == choice:
            return xapian.Query.OP_OR
        if 'and' == choice:
            return xapian.Query.OP_AND
        return None

    def make_query(self,query, field, value, choice):
        choice = self.get_choice(choice)
        if field.hasIndex():
            value = self.indexParser(field, value)
            qp = self.get_qp(field)
            filter = qp.parse_query(value)
        else:
            filter = xapian.Query(xapian.Query.OP_VALUE_RANGE, field.vno, value,value)
        return  xapian.Query(choice, query,filter)

#####################SET########################################

    def set_limit(self, offset, count):
        self._offset = offset
        self._count = count

    def set_query(self,query):
        self._query = query

    def set_sort(self, name):
        if name.find('-')==0:
            self._sort[name[1:]] = False 
        else:
            self._sort[name] = True 

    def set_range(self, name, vfrom, vto):
        self._range[name] = {'vfrom':vfrom,'vto':vto}
        field = self._scheme.getOneFields(name)
        try:
            self._range[field.vno] = xapian.Query(xapian.Query.OP_VALUE_RANGE, field.vno, vfrom, vto)
        except:
            pass

    def set_filter(self, name, value):
        self.set_range(name, value, value)

    def set_facets(self, fields):
        for i in fields:
            self._facets[i] = xapian.ValueCountMatchSpy(self._scheme.getOneFields(i).vno)
#####################SEARCH########################################

    def search(self):
        for (field, value) in self._range.items():
            filter = xapian.Query(xapian.Query.OP_VALUE_RANGE, self._scheme.getOneFields(field).vno, value['vfrom'], value['vto'])
            self._query = xapian.Query(xapian.Query.OP_FILTER, self._query, filter)
        self._xapian_enquire.set_query(self._query)

        for (field, value) in self._facets.items():
            self._xapian_enquire.add_matchspy(value)

        for (field, value) in self._sort.items():
            self._xapian_enquire.set_sort_by_value(self._scheme.getOneFields(field).vno, value)

        matches = self._xapian_enquire.get_mset(self._offset, self._count)

        result = []
        i = matches.begin()
        while not i.equals(matches.end()):
            item = {}
            doc = i.get_document()
            for (name, field) in self._scheme.getAllFields().items():
                item[name] = field.deconvert(doc.get_value(field.vno))
            result.append(item)
            i.next()

        return result,self._xapian_enquire.get_mset(0, self._xapian_read_db.get_doccount()).size()


