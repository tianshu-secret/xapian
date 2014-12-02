import xapian

class PyFieldScheme(object):
    def __init__(self):
        self._fields = {}
        self._typeMap = {}
        self._vnoMap = {}

    def getFieldID(self):
        name = self._typeMap[PyFieldMeta.TYPE_ID]
        return self._fields[name]
    
    def getFieldTitle(self):
        name = self._typeMap[PyFieldMeta.TYPE_TITLE]
        return self._fields[name]

    def getAllFields(self):
        return self._fields

    def getOneFields(self,name):
        return self._fields[name]

    def getVnoMap(self):
        return self._vnoMap

    def getTypeMap(self):
        return self._typeMap

    def addField(self, field, config = None):
        field = PyFieldMeta(field, config)
        if field.isSpeical():
            self._typeMap[field.ftype] = field.name
        field.vno = len(self._vnoMap)
        self._vnoMap[field.vno] = field.name
        self._fields[field.name] = field

class PyFieldMeta(object):
    TYPE_DATE = 'date'
    TYPE_STRING = 'string'
    TYPE_LONG = 'long'
    TYPE_FLOAT = 'float'
    TYPE_ID = 'id'
    TYPE_TITLE = 'title'

    def __init__(self, name, config):
        self.name = unicode(name, 'utf-8')
        self.ftype = u''
        self.vno = 0
        self.index = u''
        self.weight = 0
        self.fromConfig(config)

    def fromConfig(self, config):
        for (key, value) in config.items():
            if u'type' == key:
                self.ftype = value.lower()

            if u'index' == key:
                self.index = value.lower()

            if u'weight' == key:
                self.weight= value

    def hasIndex(self):
        return (self.index != u'')

    def isSpeical(self):
        return self.ftype == PyFieldMeta.TYPE_ID or self.ftype == PyFieldMeta.TYPE_TITLE

    def convert(self, data):
        if data is None:
            return data
        elif self.ftype == PyFieldMeta.TYPE_LONG:
            data = data or long(0)
            return u'%012d' % long(data)
        elif self.ftype == PyFieldMeta.TYPE_FLOAT:
            data = data or float(0)
            return xapian.sortable_serialise(float(data))
        elif self.ftype == PyFieldMeta.TYPE_DATE:
            return unicode(data.strftime("%m %d"))
        else:
            return unicode(data)

    def deconvert(self, data):
        if data is None:
            return data
        if self.ftype == PyFieldMeta.TYPE_LONG:
            data = data or long(0)
            return long(data)
        elif self.ftype == PyFieldMeta.TYPE_FLOAT:
            return xapian.sortable_unserialise(data)
        else:
            return data.decode('utf-8')
