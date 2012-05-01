import json
from types import NoneType
from collections import deque
from cStringIO import StringIO

class JSONCoder(object):
  types = (bool, str, unicode, list, dict, int, long, float, NoneType)
  def __init__(self, stream=None):
    self.stack = deque() # current root object
    if stream is None:
      stream = StringIO()    
    self.buffer = stream
    self.visited = {}
    
  @property
  def record(self):
    return self.stack[-1]
    
  def encode_root_object(self, obj):
    assert len(self.stack) == 0
    obj_id = id(obj)
    if obj_id not in self.visited:
      self.visited[obj_id] = obj
      
      # only robjects, dicts and lists are encodable as roots
      self.stack.append({})
      if isinstance(obj, list):
        self.encode_list_for(obj, '$root')
        self.stack.append(self.stack.pop()['$root'])
        
      elif isinstance(obj, dict):
        self.encode_dict_for(obj, '$root')
        self.stack.append(self.stack.pop()['$root'])
      else:
        obj.encode_with(self)
        
        
  def encode_list_for(self, l, key):
    """Ensures the given list is encodable as JSON by invoking encode_with on any elment that hase it"""

    record = []
    for i in l:
      if hasattr(i, 'encode_with'):
        self.stack.append({})
        i.encode_with(self)
        record.append(self.stack.pop())
      elif isinstance(i, dict):
        id_ = id(i)
        self.encode_dict_for(i,id_)
        record.append(self.record.pop(id_))
      elif isinstance(i, list):
        id_ = id(i)
        self.encode_list_for(i,id_)
        record.append(self.record.pop(id_))
      else:
        record.append(i)
    self.record[key]=record
  
  def encode_dict_for(self, d, key):
    self.stack.append({})
    record = self.record
    for k,v in d.items():
      if hasattr(v, 'encode_with'):
        self.stack.append({})
        v.encode_with(self)
        record[k] = self.stack.pop()
      elif isinstance(v, dict):
        id_ = id(v)
        self.encode_dict_for(v,id_)
        record[k] = self.record.pop(id_)
      elif isinstance(v, list):
        id_ = id(v)
        self.encode_list_for(v,id_)
        record[k] = self.record.pop(id_)
      else:
        record[k] = v
      
    self.record[key] = self.stack.pop() 

    
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
    if hasattr(object, 'encode_with'):
      # TODO: this only makes sense if we actually
      # tracked all objects in the dict and then encoded a reference
      self.stack.append({})
      self.record[key] = self.stack.pop()
    else:
      # Hope the object is json encodeable
      self.record[key] = object
          
  def encode_set_for(self, set, key):
    self.record[key] = tuple(set)
    
  def finish_encoding(self):
    if len(self.stack) > 0:
      json.dump(self.stack[0], self.buffer)
      self.buffer.write('\n')
