import uuid
import inspect
from collections import defaultdict

from Rambler import outlet, option, component, nil, field,coroutine
from Rambler.Entity import RObject



class Entity(RObject):
  RunLoop = outlet('RunLoop')
  component_registry = outlet('ComponentRegistry')
  store_conf = option('storage','conf')
  event_service = outlet('EventService')

  en_inflector = outlet('EnglishInflector')
  
  one = outlet('one')
  many = outlet('many')
  
  
  _store = None
  _is_new = False
  
  roles_by_class = defaultdict(dict)
  relations_by_class = defaultdict(dict)
  
  errors = nil
  
  # Name of fields that act as the primary key
  primary_key_fields = ['id']
  
  @classmethod
  def assembled(cls):
    cls.event_service.registerEvent('create',Entity, Entity)
    cls.event_service.registerEvent('update',Entity, Entity)
    cls.event_service.registerEvent('remove',Entity, object)
    
    # relation events post a tuple containing object, object, relation
    cls.event_service.registerEvent('relate',Entity,  object)
    cls.event_service.registerEvent('unrelate',Entity, object)

  @classmethod
  def relation_role_for(cls, name):
    return cls.roles_by_class[cls][name]
    
  @classmethod
  def belongs_to(cls, name, **options):
    setattr(cls, name, cls.one(cls, name, ownership='belongs', **options))

  @classmethod
  def has_one(cls, name, **options):
    setattr(cls, name, cls.one(cls, name, ownership='has', **options))
    
  @classmethod
  def has_many(cls, name, **options):
    setattr(cls, name, cls.many(cls, name, ownership='has', **options))
    
  @classmethod
  def has_and_belongs_to_many(cls, name, **options):
    setattr(cls, name, cls.many(cls, name, ownership='has', **options))
        
  @property
  def store(self_or_cls):
    if isinstance(self_or_cls, type):
      cls = self_or_cls
    else:
      cls = self_or_cls.__class__
    
    if cls._store is None:
      default_name = cls.store_conf.get('default', None)
      class_name = cls.__name__
      store_name = cls.store_conf.get(class_name, default_name)
      cls._store = cls.component_registry.lookup(store_name)
    return cls._store

  @classmethod
  def fields(cls):
    one = cls.one
    if not hasattr(cls, '_fields'):
      cls._fields = {}
      for name, field_instance in inspect.getmembers(cls, lambda f: isinstance(f, field) or isinstance(f,one)):
          
        field_instance.name = name
        cls._fields[name] = field_instance
    return cls._fields

  @property
  def attributes(self):
    return self.fields().keys()
    
    
  @classmethod
  def new (cls, **kw):
    instance = cls()      
    instance.set_values(kw)
    instance._is_new = True
    return instance
    
  @classmethod
  def create(cls, **kw):
    instance = cls()      
    instance.set_values(kw)
    instance._is_new = True
    return instance.save()
    
    
  @classmethod
  def find(cls, retreival, order=None, **conditions):
    return cls.store.fget(cls).find(cls, retreival, order, **conditions)

    
  @classmethod
  def maximum(cls, column_name, **conditions):
    return cls.store.fget(cls).maximum(cls, column_name, **conditions)
    
  @classmethod
  def count(cls, column_name='*', **conditions):
    return cls.store.fget(cls).count(cls, column_name, **conditions)


  # Relationship methods
  @coroutine
  def relate(self, related_obj, relation):
    run_loop = self.RunLoop.currentRunLoop()
    op = self.store.relate(self, related_obj, relation)

    op.add_observer(self, 'is_finished', 0,  
      run_loop.callFromThread, 
        self.event_service.publish,
          'relate', Entity, (related_obj, self, relation))
          
    return op

  @classmethod
  @coroutine
  def create_related(cls, related_obj, relation, **kw):
    instance = yield cls.create(**kw)
    yield cls.store.fget(cls).relate(instance, related_obj, relation)
    
  @classmethod
  def find_related(cls, related_obj, relation, *args, **conditions):
    return cls.store.fget(cls).find_related(related_obj, relation, *args, **conditions)

  @classmethod
  def count_related(cls, related_obj, relation):
    """Return the count of the objects related to related_object"""
    return cls.store.fget(cls).count_related(related_obj, relation)
   

    
  
  @classmethod
  def init_with_coder(cls, coder):
    obj = cls()
    for field_name, field in cls.fields().items():
      try:
        decode_method_name = 'decode_%s_for' % field.type.__name__
        # attempt to call encode_type_for() the given type, for examlp
        # encode_int_for(...) if the value is an int. If the coder does
        # support the specific type we use the generic to encode_object_for(...)
        # method
        decode_val_for_key = getattr(coder, decode_method_name, coder.decode_object_for)
        obj.set_value_for_key(decode_val_for_key(field_name), field_name)
      except:
        cls.log.exception('Exception encountered decoding %s as %s', field.name, field.type)
        raise
    return obj
    
  def encode_with(self, coder):
    """Introspect the given object and returns a dictionary of values that should be persisted"""

    for field_name, field in self.fields().items():
      try:
        encode_method_name = 'encode_%s_for' % field.type.__name__
      except AttributeError:
        # ignore relations
        continue
      # attempt to call encode_type_for() the given type, for example
      # encode_int_for(...) if the value is an int. If the coder does
      # support the specific type we use the generic to encode_object_for(...)
      # method
      encode_val_with_key = getattr(coder, encode_method_name, coder.encode_object_for)
      value = field.__get__(self, self.__class__)
      encode_val_with_key(value, field_name)

  
  def save(self):
    self.validate()
    if self.errors:
      raise RuntimeError(self.errors)

    run_loop = self.RunLoop.currentRunLoop()
    if self._is_new:
      # Todo: What relies on auto id now? This should be moved to the storage classes
      # or set as a default
      if hasattr(self, 'id') and self.id is None:
        self.id = str(uuid.uuid1())
        
      op = self.store.create(self)
      op.add_observer(self, 'is_finished', 0,  run_loop.callFromThread, self.event_service.publish, 'create', Entity, self)

      
      return op
    else:
      op = self.store.update(self)
      op.add_observer(self, 'is_finished', 0,  run_loop.callFromThread, self.event_service.publish, 'update', Entity, self)
      return op
      
  @property
  def primary_key(self):
    key = []
    for field in self.primary_key_fields:
      key.append(getattr(self, field))
    if len(key) == 1:
      return key[0]
    else:
      return tuple(key)
    
  def observe_value_for(self, keypath, operation, changes, callback, *args, **kw):
    operation.remove_observer(self, keypath)
    callback(*args, **kw)


  def __init__(self, **kw):
    self.attr = {}
    super(Entity,self).__init__(**kw)
    
    
  def set_value_for_key(self, value, key):
    """Entities will only set values on fields or relationships"""
    
    self.will_change_value_for(key)
    
    if key in self.fields():
      setattr(self,key,value)
    else:
      self.set_value_for_undefined_key(value, key)
    self.did_change_value_for(key)

    
  def __getitem__(self, key):
    return self.attr[key]

  def __setitem__(self, key, val):
    self.attr[key] = val
      
  def __getattr__(self, attribute):
    if not attribute.startswith('_'):
      return self.value_for_undefined_key(attribute)
    else:
      raise AttributeError(attribute)
      
    
  def validate(self):
    pass
  
  