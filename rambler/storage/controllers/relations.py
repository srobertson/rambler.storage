from Rambler import outlet, component,coroutine

    
__guess__ = ()
  

      

class one(object):
  """ Proxy for a related entity
  
  Usage:
  >>> object.other -> returns an operation
  >>> object.other.x -> returns the value of x from the other attribute, 
                        will cache other values
  >>> object.other.select(attr1,attr2,attr3) returns an op only retreiving a specific value
  values
  
  >>> object.other.refresh()
  >>> object.other.refresh().x
  
  Noromal usage:
  >>> other = yield obj.other
  # Permofrm operotaion on entities
  
  
  
  """
  comp_reg     = outlet('ComponentRegistry')
  scheduler    = outlet('Scheduler')
  en_inflector = outlet('EnglishInflector')
  cardinality  = "one"
    
  @classmethod
  def assembled(cls):
    relation.scheduler = cls.scheduler
    
  def __init__(self, model, name,  **options):
    self.model         = model
    self.name          = name
    
    # Inflector improperly singularizes names that end with s 
    # like status/address so pluarlize before calling classify
    
    plural = self.en_inflector.pluralize(name)
    self.destination_name = options.get('destination',  self.en_inflector.classify(plural))
    self.ownership     = options['ownership']
    self.inverse_name  = options.get('inverse', __guess__)
    self._destination = None
    self._inverse = None
    
  def __get__(self, obj, objtype):
    if obj is None: # Access from the class level return the descriptor
      return self
    else:
      # TODO: Return an operation that will fetch the object or a cached copy
      #return self.destination.find_related(obj, self)
      return  relation(obj,self)
      
  def __set__(self, obj,  value):

    obj.attr[self.name] = value
    inverse = self.inverse
    if inverse:
      setattr(value, inverse.name, obj)
  
  @property
  def destination(self):
    """Returns the destination model"""
    if self._destination is None:
      self._destination = self.comp_reg.lookup(self.destination_name)
    return self._destination
          
  @property  
  def inverse(self):
    # lookup the inverse and cache it
    if self._inverse is None:
      for guess in self.guess_inverse():

        other = getattr(self.destination, guess, None)
        if other:
          self._inverse = other
          break
      #assert self._inverse, "Missing inverse for %s" % self.name

    return self._inverse
    
  
    
  def guess_inverse(self):
    """Returns one or more potential roles that could be the inverse of this one"""
    if self.inverse_name is not __guess__:
      # inverse has been explicitly been  specified no need to guess
      return (self.inverse_name,)
    else:
      singular = self.model.__name__.lower()
      return  (singular, self.en_inflector.pluralize(singular))
  
        
class many(one):
  cardinality   = "many"
  def wrap(self, obj):
    if self.name not in obj.attr:
      obj.attr[self.name] = collection(obj, self)
    return obj.attr[self.name]
    
  def __get__(self, obj, objtype):
    if obj is None:
      return self
    return self.wrap(obj)
    
  def __set__(self, obj,  value):
    self.wrap(obj).add(value)
    
class relation:
  """Manages the relation between two objects"""
  
  def __init__(self, obj, relation):
    self.obj = obj
    self.relation = relation
    self.destination = relation.destination
  
  def __call__(self):
    return self.find()
    
  # Return operations for creating and manipulating the related object
  @coroutine
  def create(self, *args, **kw):
    entity = yield self.destination.create(*args, **kw)
    yield self.set(entity)
    yield entity
  
  @coroutine  
  def find(self):
    if self.obj.attr.has_key(self.relation.name):
      yield self.obj.attr[self.relation.name]
    else:
      yield self.destination.find_related(self.obj, self.relation)
    
  def set(self, other):
    return other.relate(self.obj, self.relation)

  def remove(self, other):
    return other.unrelate(self.obj, self.relation)

    
class collection:
  """Collection of objects, relies on the storage to fill in self.values
  """
  def __init__(self, obj, relation):
    self.obj = obj
    self.relation = relation
    self.values = set()
    
  def __call__(self):
    return self.all()
    
  def __iter__(self):
    return self.all()

  def all(self):
    return self.find('all')
    
    #self.model.find_related(self.obj, self.relation)
    #q = {self.foreign_key: self.obj.primary_key}
    #return self.model.find('all', conditions=q)
    
  def add(self, obj):
    #self.model.relate(obj)
    #obj[self.foreign_key] = self.obj.primary_key
    # Return the op incase some one has the urge to wait for it
    #return obj.save()
    self.values.add(obj)
    #return self.obj.relate(obj, self.relation)
  
  def create(self, **kw):
    op = self.relation.destination.create_related(self.obj, self.relation, **kw)
    return op
    #kw[self.foreign_key] = self.obj.primary_key
    #return self.model.create(**kw)
    
  def find(self, *args, **conditions):
    return self.relation.destination.find_related(self.obj, self.relation, *args, **conditions)
    #conditions[self.foreign_key] = self.obj.primary_key
    #return self.model.find(*args, conditions=conditions)
    
  def count(self):
    destination = self.relation.destination
    return destination.count_related(self.obj, self.relation)
      
  