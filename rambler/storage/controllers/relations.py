from Rambler import outlet, component

class one(object):
  comp_reg            = outlet('ComponentRegistry')
  InvocationOperation = outlet('InvocationOperation')
  
  def __init__(self, model_name, foreign_key = None):
    self.model_name = model_name
    self._model = None

    self.attr_name = model_name.lower()
    
    self.foreign_key = foreign_key
    
  @property
  def model(self):
    if self._model is None:
      self._model = self.comp_reg.lookup(self.model_name)
    return self._model
    
class many(one):
    
  def __get__(self, obj, objtype):
    if obj is None:
      return
      
    if self.foreign_key is None:
      foreign_key = self.attr_name + '_id'
    else:
      foreign_key = self.foreign_key
      
    if self.attr_name not in obj.attr:
      obj.attr[self.attr_name] = wrapper(obj, self.model, foreign_key)
    return obj.attr[self.attr_name]
    
class wrapper():
  def __init__(self, obj, model, foreign_key):
    self.obj = obj
    self.foreign_key =  foreign_key
    self.model = model
    
  def __call__(self, *args, **kw):
    return self.all(*args, **kw)
    
  def create(self, **kw):
    kw[self.foreign_key] = self.obj.primary_key
    return self.model.create(**kw)
    
  def find(self, *args, **constraints):
    constraints[self.foreign_key] = self.obj.primary_key
    return self.model.find(*args, **constraints)
    
  def all(self):
    q = {self.foreign_key: self.obj.primary_key}
    return self.model.find('all', **q)
      
  def count(self):
    return self.model.count()
  