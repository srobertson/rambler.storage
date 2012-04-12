import itertools
from searchable  import STable

class UnitOfWork(object):
  NOT_EXIST = 0
  NEW       = 1
  CLEAN     = 2
  DIRTY     = 3
  REMOVED   = 4

  def __init__(self):
    self.table = STable()
    self.table.create_index('pirmary_key')
    self.table.create_index('store')
    self.table.create_index('state')
    
  def register_clean(self, obj):
    """Registers an object that is unchanged from the database. An
    object can not be registered as clean if it exists in any
    other state."""
    
    self.__register(obj, self.CLEAN)
              
  def __register(self,obj, state,allowed_states=[]):
    pk = (type(obj),obj.primary_key)
    old = self.table.where(primary_key=pk).first()
    if(old and old.state not in allowed_states):
       raise ValueError, "Object with the primary key of %s" \
              "already registered with the following status %s" % (pk,state)
    else:
      self.table.insert({'obj':obj, 'primary_key':pk, 'store':obj.store, 'state':state})
              
              
  def register_dirty(self, obj):      
    """Marks an object as being modified in the current
    transaction and needing to be updated. An object can only be
    registered as dirty if it's been previously registered as
    clean."""
    self.__register(obj, self.DIRTY, allowed_states=(self.CLEAN))

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


  def get_status(self, primary_key):
    """Returns the status of the given object. The status can be either
    - NEW
    - DIRTY
    - REMOVED
    - CLEAN
    - NOT_EXIST
    """
    
    pk = (type(obj),obj.primary_key)
    return self.table.where(primary_key=pk).first().state


  def get(self, primary_key, default=None):
    """Returns the given object or the default value if the object
    doesn't exist."""
    
    record = self.table.where(primary_key=primary_key).first()
    if obj:
      return record['obj']
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
    return [record["obj"] for record in self.table.where(**kw).all()]
    
  def objects(self):
    "Returns a list of all objects in the current unit of work"
    return [record["obj"] for record in self.table.all()]
    
  def stores(self):
    """Returns a list of all the stores that participated in the current
    transaction."""
    return self.table.indexes["store"].keys()
  