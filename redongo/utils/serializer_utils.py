try:
    import ujson
    import json
except:
    import json

try:
    import cPcikle as pickle
except:
    import pickle


class serializer():
    def __init__(self, serializer_type):
        if serializer_type == 'ujson':
            self.loads = ujson.loads
            self.dumps = ujson.dumps
        elif serializer_type == 'json':
            self.loads = json.loads
            self.dumps = json.dumps
        else:
            self.loads = pickle.loads
            self.dumps = pickle.dumps
