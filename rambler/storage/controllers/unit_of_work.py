import itertools
from searchable  import STable

from searchable  import STable
from Rambler import coroutine

def attrgetter(obj, key):
  return getattr(obj, key, None)
  
class UnitOfWork(object):
  NOT_EXIST = 0
  NEW       = 1
  CLEAN     = 2
  DIRTY     = 3
  REMOVED   = 4
  RELATE    = 5
  UNRELATE  = 6

  def __init__(self):
    self.table = STable(key=attrgetter)
    self.table.create_index('primary_key')
    self.table.create_index('__class__')
    
    self.table.create_index('store')
    self.table.create_index('_Entity__state')
    
    self._changes = STable()
   
  def observe_value_for(self, key_path, obj, changes):
    if changes[obj.KeyValueChangeKindKey] == obj.KeyValueChangeInsertion:
      self._changes.insert({'type': 'relate', 'object': obj, 'relation': key_path,  'values':  changes[obj.KeyValueChangeNewKey]})
      
    elif obj.is_clean():
      obj._Entity__state = obj.DIRTY
      self._changes.insert({'type': 'update', 'object': obj, 'set': {key_path: obj.attr[key_path]}})
      self.register_dirty(obj)
    # todo: track set mutations as individual changes
    else:
      value = obj.attr[key_path]
      # since update syntaxt does not traverse objects or hashes, lookup
      # and add change directly to the mutations list
      self._changes.where(object=obj).first()[key_path] = value
    

  def changes(self):
    return self._changes.where()
    
  @coroutine
  def find(self, model, retrieval, order=None, limit=None, conditions=None, **kw):
    conditions = conditions or kw
    order = order or ()
    
    if retrieval not in ('first','all'):
      # looking for the primary_key tracking the object in memory already      
      entity = self.table.where(__class__=model, primary_key=retrieval).first()
      if entity:
        yield entity
        return
    else:
      records = yield model.store.fget(model).find(model, retrieval, 
         order=order, limit=limit, conditions=conditions, **kw)
         
      cache = self.table.where(**conditions).order_by(*order)
         
      yield itertools.ifilterfalse(lambda e: e.is_removed(), 
        self.merge(iter(records), cache))
  
  def merge(self, records, cache):
    # TODO: if the object is in both the cache and records we should defer to the cache copy
    try:
      record = None
      top = None
    
      while(1):
        if record is None:
          record = records.next()

        if top is None:
          top = cache.next()
           
        res = cmp(record,top)
        if res == -1:
          yield self.realize(record)
          record = None
        elif res == 0:
          yield self.realize(record)
          yield top
          record = None
          top = None
        else:
          yield top
          top = None
    except StopIteration:
      if top is not None:
        yield top
      if record is not None:
        yield self.realize(records)
        
    # only one of these will have items left to return    
    for i in cache:
      yield i
  
    for r in records:
      yield self.realize(r)
        
  def relate(self, obj, relation, other):
    pk = self.pk(obj)
    other_pk = self.pk(other)
    ref = {'primary_key': pk, 'other_key': other_pk, 'relation': relation.name}
    self.table.insert(ref)

    
    
  def realize(self, model, record):
    #Optimization hint, go straight to the index for this one
    pk = self.pk_from_record(model, record)

    entity = self.table.where(__class__=model, primary_key=pk).first()
    if entity is None:
      entity = model(**record)
      self.register_clean(entity)
    else:
      entity = entity
    return entity
      
  def register_clean(self, obj):
    """Registers an object that is unchanged from the database. An
    object can not be registered as clean if it exists in any
    other state."""
    
    self.__register(obj, self.CLEAN)
              
  def __register(self,obj, state,allowed_states=[]):
    old = self.table.where(__class__=type(obj), primary_key=obj.primary_key).first()
    if old:
      if old._Entity__state not in allowed_states:
        raise ValueError, "Object with the primary key of %s" \
          "already registered with the following status %s" % (obj.primary_key,state)
      else:
        #Optimize hint, old is the row_id, so we could avoid a double search
        self.table.update().set(state=state).where(__class__=type(obj), primary_key=pk).execute()
    else:
      obj.add_observer(self, '*', obj.KeyValueObservingOptionOld | obj.KeyValueObservingOptionNew)
      self.table.insert(obj)

              
              
  def register_dirty(self, obj):      
    """Marks an object as being modified in the current
    transaction and needing to be updated. An object can only be
    registered as dirty if it's been previously registered as
    clean."""

    self.__register(obj, self.DIRTY, allowed_states=(self.CLEAN,))

  def register_removed(self, obj):

    """Marks an object as needing to be removed at the end of the
    transaction. An object can only be registered as removed if it
    was registered as new, clean or dirty prior to being
    removed. """

    self.__register(obj, self.REMOVED, allowed_states=(self.NEW,self.CLEAN, self.DIRTY))

  def register_new(self, obj):
    """Marks an object as being newly created in the current
    transaction. An object can only be registered new if it has no
    previous state."""
        
    self.__register(obj, self.NEW)
    self._changes.insert({'type':'create', 'object': obj})


  def get_status(self, primary_key):
    """Returns the status of the given object. The status can be either
    - NEW
    - DIRTY
    - REMOVED
    - CLEAN
    - NOT_EXIST
    """
    
    return self.table.where(primary_key=self.pk(obj)).first().state


  def get(self, primary_key, default=None):
    """Returns the given object or the default value if the object
    doesn't exist."""
    
    record = self.table.where(primary_key=primary_key).first()
    if obj:
      return record
    else:
      return default
    
  def get_new(self):
    """Returns a list of all new objects in the current
    transaction."""
    return self.table.where(state=self.NEW).all()
      
  def get_clean(self):
    """Returns a list of all the clean objects in the current
    transaction."""
    return self.table.where(state=self.CLEAN).all()

  def get_dirty(self):
    """Returns a list of all the dirty objects in the current
    transaction."""
    return self.table.where(state=self.DIRTY).all()

  def get_removed(self):
    """Returns a list of all objects that need to be removedi in
    the current transaction."""
    return self.table.where(state=self.REMOVED).all()

  def clean(self):
    self.table.delete().where(state=self.REMOVED).execute()
    self.table.update().set(state=self.CLEAN).execute()
    
    
  def clear(self):
    """Removes all the object from every possible state."""
    self.table.delete().execute() 
  rollback = clear
  
  def where(self,**kw):
    """Returns a list of objects that match the where clause"""
    return self.table.where(**kw).all()
    
    
  def pk(self, obj):
    """Returns a tuple suitable for locating the object in the memory storage"""
    return obj.primary_key
    
  def pk_from_record(self, model, record):
    """Returns the primary_key from the record"""
    if len(model.primary_key_fields) == 1:
      # optimization for single key primary keys
      return record[model.primary_key_fields[0]]
    else: #it's a commpond key      
      primary_key = []
      for field in model.primary_key_fields:
        primary_key.append(mutations[field])
      return tuple(primary_key) # make it a tuple so it can be hashed
    
  def objects(self):
    "Returns a list of all objects in the current unit of work"
    return  self.table.all()
    
  def stores(self):
    """Returns a list of all the stores that participated in the current
    transaction."""
    return self.table.indexes["store"].keys()
  