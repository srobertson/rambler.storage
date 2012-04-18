import json
from collections import deque
from cStringIO import StringIO

class JSONCoder(object):
  def __init__(self):
    self.stack = deque() # current root object
    self.buffer = StringIO()
    self.buffer.write('[')
    self.visited = set()
    self.record = None
    
  def encode_root_object(self, obj):
    if obj not in self.visited:
      self.visited.add(obj)
      self.record = {}
      self.stack.append(self.record)
      obj.encode_with(self)
      rec = self.stack.pop()
      self.buffer.write(json.dumps(rec))
      self.buffer.write(',')
      try:
        self.record=self.stack[-1]
      except IndexError: # top of the stack
        self.record = None
    
  def encode_int_for(self, i, key):
    self.record[key] = i
    
  def encode_str_for(self, string, key):
    self.record[key] = string
    
  def encode_bool_for(self, bool, key):
    self.record[key] = bool
    
  def encode_datetime_for(self, dt, key):
    if dt:
      self.record[key] = dt.isoformat()
  
  def encode_object_for(self, object, key):
    if isinstance(object, self.Entity):
      #assert object._is_new == False
      self.record[key] = {'$ref': object.__name__, '$id': object.primary_key}
      self.encode_root_object(object)
    else:
      # Hope the object is json encodeable
      self.record[key] = object
          
  def encode_set_for(self, set, key):
    self.record[key] = tuple(set)
    
  def finish_encoding(self):
    # look back one character
    self.buffer.seek(-1,2)
    if self.buffer.read(1) == ',':
      self.buffer.seek(-1,2)
    self.buffer.write(']')
  