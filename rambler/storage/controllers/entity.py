import sys
import uuid
import inspect
from collections import defaultdict

from Rambler import outlet, option, component, nil, field,coroutine
from Rambler.robject import RObject



class Entity(RObject):
  #TODO: These are redunant with the constants in the uow
  # determine who owns state
  NOT_EXIST = 0
  NEW       = 1
  CLEAN     = 2
  DIRTY     = 3
  REMOVED   = 4
  
  RunLoop = outlet('RunLoop')
  component_registry = outlet('ComponentRegistry')
  store_conf = option('storage','conf')
  event_service = outlet('EventService')
  log = outlet("LogService")
  en_inflector = outlet('EnglishInflector')
  UnitOfWork = outlet('UnitOfWork')
  
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
  def will_disassemble(cls):
    # Clear cached store
    cls._store = None
    #cls._fields = None
    
  @classmethod
  def uow(cls):
    try:
      uow = coroutine.context.rambler_storage_uow
    except AttributeError:
      uow = coroutine.context.rambler_storage_uow = cls.UnitOfWork()
    return uow
    
  @classmethod
  @coroutine
  def commit(cls):
    """Commit changes made to any object in this context."""
    uow = cls.uow()

    try:
      for store in uow.table.indexes['store']:
        # optimization hint, if we have multiple stores
        # it may be benificial to run commit on all stores
        # in parallel
        yield store.commit(uow)
      
      uow.clean()

    except:
      exc_info = sys.exc_info() #store orginal error incase the storage has a problem
      for store in uow.stores():
        try:
          yield store.rollback()
        except:
          # storages should not throw exceptions during rollback, not much we can
          # do if the error handler has an error except log it
          cls.log.exception("Exception encountered  rollingback store %s", store)
      uow.rollback()
      # throw the original exception
      raise exc_info[0],exc_info[1],exc_info[2]
    
    uow.clean()

    
    
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
    if instance.primary_key is None:
      instance.id = str(uuid.uuid1())
    cls.uow().register_new(instance)
    
    instance.__state = cls.NEW
    # todo: remove _is_new, don't think it's needed
    instance._is_new = True
    return instance #instance.save()
    
    
  @classmethod
  def find(cls, retreival, order=None, **conditions):
    records = cls.store.fget(cls).find(cls, retreival, order, **conditions)

    return self.uow.realize(records)

    
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



  # state querying methods
  def is_new(self):
    return self.__state == self.NEW
    
  def is_clean(self):
    return self.__state == self.CLEAN
    
  def is_dirty(self):
    return self.__state == self.DIRTY
    
  def is_removed(self):
    return self.__state == self.REMOVED
  
    
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
    self.__state = self.CLEAN
    self.attr = {}
    super(Entity,self).__init__(**kw)
    
  #def value_for_key(self, key):
  #  field = self.fields()[key]
  #  return getattr(self, field.name)
    
  def set_value_for_key(self, value, key):
    """Entities will only set values on fields or relationships"""


    field = self.fields().get(key)
    
    if field:
      if field.is_relation: # relations will handle will/did_change_value calls 
        field.relate(self, value)
        inverse = field.inverse
        if inverse:
          inverse.relate(value, self)
      else:
        self.will_change_value_for(key)
        self.attr[key] = value
        self.did_change_value_for(key)
    else:
      self.will_change_value_for(key)
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

  