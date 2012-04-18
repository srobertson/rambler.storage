import itertools

from collections import defaultdict
from Rambler import component, coroutine
from Rambler.LRU import LRU

class InMemoryStorage(object):
  storage_by_class = defaultdict(lambda: LRU(10))
    
  @classmethod
  def will_disassemble(self):
    self.storage_by_class.clear()

  def prepare(self, uow):
    pass
    
  @coroutine
  def commit(self, uow):
    for obj in uow.where(store=self, state=uow.NEW):
      self.storage_by_class[type(obj)][obj.primary_key] = obj.attr.copy()
    
    for obj in uow.where(store=self, state=uow.DIRTY):
      self.storage_by_class[type(obj)][obj.primary_key] = obj.attr.copy()
    
    for obj in uow.where(store=self, state=uow.REMOVED):
      del self.storage_by_class[type(obj)][obj.primary_key]
      
  def rollback(self):
    pass
  
  def create(self, obj):
    """Copy attributes to the in memory storage"""
    #relpace this the JSONCoder
    self.record = {}
    obj.encode_with(self)
    self.storage_by_class[type(obj)][obj.primary_key] = self.record
    obj._is_new = False
    
  save = update = create
  
  def find(self, model, retrieval, order=None, limit=None, conditions=None, **kw):
    conditions = conditions or kw

    #op = self()
    objects = self.storage_by_class[model]
    
    if retrieval not in ('first', 'all'):
      # retrieval is an id, create an instace and apply the attrs
      return objects[retrieval]
       
    def matches(record):
      for key,val in conditions.items():
        if record[key] != val:
          return False
      return True

    records = filter(matches, objects.values())
    if retrieval == 'first':
      if records:
        return records[0]
    else:
      return records

  @classmethod
  @coroutine
  def find_related(self, entity, relation, *args, **conditions):
    # TODO: this should mimic the entity.find behavior

    if relation.cardinality == 'many':
      yield entity.attr.get(relation.name,{}).values
    else:
      yield  entity.attr.get(relation.name)



  @classmethod
  def count(self, model, column_name='*', conditions=None):
    op = self.find(model, "all",  conditions=conditions)
    op.block = len
    return op
    
  @classmethod
  def maximum(self, model, column_name, conditions=None):
    op = self.find(model, "all", order=column_name, limit=1, conditions=conditions)
    def first_column(records):
      if len(records):
        return records[0][column_name]
    
    op.block = first_column
    return op
    

  def __init__(self):
    super(InMemoryStorage, self).__init__()
  
  def main(self):
    pass

  
  def encode_object_for(self, object, key):
    '''Copy keys that an object wishes serialized to an in memory dictionary'''
    self.record[key] = object
  
  @property
  def result(self):
    if self.block:
      return self.block(self.records)
    else:
      return self.records