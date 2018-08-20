class Schema(object):
    def __init__(self, name):
        self.schema_name = name

    def name(self):
        return self.schema_name

class Attr(object):
    def __init__(self, schema, attr_name, attr_id, attr_type):
        self.schema_ = schema
        self.name_ = attr_name
        self.attr_id_ = attr_id
        self.attr_type = attr_type

    def schema(self):
        return self.schema_

    def name(self):
        return self.name_

    def attr_id(self):
        return self.attr_id_

    def type(self):
        return self.attr_type_
