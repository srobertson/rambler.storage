import itertools

class UnitOfWork(object):
  NOT_EXIST = 0
  NEW       = 1
  CLEAN     = 2
  DIRTY     = 3
  REMOVED   = 4

  def __init__(self):
    self.clear()
        
    
  def register_clean(self, obj):
    """Registers an object that is unchanged from the database. An
    object can not be registered as clean if it exists in any
    other state."""


    pk = obj.primary_key
    status = self.get_status(pk)
    if status == self.NOT_EXIST:
      self._objects[self.CLEAN][pk] = obj
      self._obj_states[pk] = self.CLEAN
    else:
      raise ValueError, "Object with the primary key of %s" \
            "already registered with the following status %s" % (pk,status)
        

  def register_dirty(self, obj):      
    """Marks an object as being modified in the current
    transaction and needing to be updated. An object can only be
    registered as dirty if it's been previously registered as
    clean."""

    pk = obj.primary_key
    status = self.get_status(pk)
    if status == self.CLEAN:
      obj = self.get(pk)
      del self._objects[status][pk] 
      self._objects[self.DIRTY][pk] = obj
      self._obj_states[pk] = self.DIRTY
    else:
      raise ValueError, "Object with the primary key of %s" \
            " with an invalid status of %s" % (pk,status)


  def register_removed(self, obj):

    """Marks an object as needing to be removed at the end of the
    transaction. An object can only be registered as removed if it
    was registered as new, clean or dirty prior to being
    removed. """

    pk = obj.primary_key
    status = self.get_status(pk)
    if status != self.NOT_EXIST:
      obj = self.get(pk)
      del self._objects[status][pk] 
      self._objects[self.REMOVED][pk] = obj
      self._obj_states[pk] = self.REMOVED
    else:
      raise ValueError("Object with the primary key of %s, "
                       "has not been registered." % pk)

  def register_new(self, obj):
    """Marks an object as being newly created in the current
    transaction. An object can only be registered new if it has no
    previous state."""

    # TODO:most of the time obj won't have a primary key
    # need to create a temp key. storage objects need away
    # to retreive keys when they set them
    pk = obj.primary_key
    status = self.get_status(pk)
    if status == self.NOT_EXIST:
      self._objects[self.NEW][pk] = obj
      self._obj_states[pk] = self.NEW
    else:
      raise ValueError("Object with the primary key of %s" 
            " already registered with the following status %s" % (pk,status))

  def get_status(self, primary_key):
      """Returns the status of the given object. The status can be either
      - NEW
      - DIRTY
      - REMOVED
      - CLEAN
      - NOT_EXIST
      """
      return self._obj_states.get(primary_key, self.NOT_EXIST)

  def get(self, primary_key, default=None):
    """Returns the given object or the default value if the object
    doesn't exist."""
    status = self.get_status(primary_key)
    if status == self.NOT_EXIST:
        return default
    else:
        return self._objects[status][primary_key]

  def get_new(self):
    """Returns a list of all new objects in the current
    transaction."""
    return self._objects[self.NEW].values()
      
  def get_clean(self):
    """Returns a list of all the clean objects in the current
    transaction."""

    return self._objects[self.CLEAN].values()

  def get_dirty(self):
    """Returns a list of all the dirty objects in the current
    transaction."""
    return self._objects[self.DIRTY].values()

  def get_removed(self):
    """Returns a list of all objects that need to be removedi in
    the current transaction."""
    return self._objects[self.REMOVED].values()

  def clean(self):
    self._objects[self.CLEAN].update(self._objects[self.NEW])
    self._objects[self.NEW].clear()
    self._objects[self.CLEAN].update(self._objects[self.DIRTY])
    self._objects[self.DIRTY].clear()
    self._objects[self.REMOVED].clear()
    
    for pk,state in self._obj_states.items():
      if state == self.REMOVED:
        del self._obj.states[pk]
      else:
        self._obj_states[pk] = self.CLEAN
    
  def clear(self):
    """Removes all the object from every possible state."""
    self._objects = {
        self.NEW:{},
        self.CLEAN:{},
        self.DIRTY: {},
        self.REMOVED:{}
    }

    self._obj_states = {}
  rollback = clear
  def objects(self):
    # flatten all objects
    return list(itertools.chain(*self._objects.values()))


