import itertools

from collections import defaultdict
from Rambler import component, option

import searchable

class MutableStorage(component('Operation')):
  block = None
  is_concurrent=True
  
  @classmethod
  def assembled(cls):
    cls.rebase()
    cls.storage_by_class = defaultdict(searchable.dict)
    
  @classmethod
  def will_disassemble(cls):
    cls.storage_by_class.clear()
  
  @classmethod
  def create(cls, obj):
    """Copy attributes to the in memory storage"""
    
    operation = cls()
    operation.records = [obj]
    
    def create_obj(records):
      cls.storage_by_class[type(obj)][obj.primary_key] = obj
      obj._is_new = False
      return obj
      
    operation.block = create_obj
    
    return operation
    
  update = create
  
  @classmethod
  def find(cls, model, retrieval, order=None, limit=None, conditions=None, **kw):

    conditions = conditions or kw

    op = cls()
    objects = cls.storage_by_class[model]
    
    if retrieval not in ('first', 'all'):
      op.records = objects[retrieval]
      return op
      
    def matches(record):
      for key,val in conditions.items():
        if record[key] != val:
          return False
      return True

    op.records = filter(matches, objects.values())
    if retrieval == 'first':
      def first(records):
        if records:
          return records[0]
      op.block = first
    else:
      def lazy_map_all(records):
        return records #map(lambda r: model(**r), records)
      op.block = lazy_map_all
     
    return op   
    #attributes = cls.storage_by_class[model][retrieval]
    #return model(**attributes)
    


  @classmethod
  def count(cls, model, column_name='*', conditions=None):
    op = cls.find(model, "all",  conditions=conditions)
    op.block = len
    return op
    
    
  @classmethod
  def maximum(cls, model, column_name, conditions=None):
    op = cls.find(model, "all", order=column_name, limit=1, conditions=conditions)
    def first_column(records):
      if len(records):
        return records[0][column_name]
    
    op.block = first_column
    return op
    

  ## Operations on relationships
  @classmethod
  def count_related(cls, entity, relation):
    op = cls()
    op.records = entity.attr[relation.name].values
    op.block = len
    return op
    
  @classmethod
  def find_related(cls, entity, relation, *args, **conditions):
    op = cls()
    
    if relation.cardinality == 'many':
      op.records = entity.attr[relation.name].values
    else:
      op.records =  entity.attr[relation.name]
    return op
    
  @classmethod
  def relate(cls, entity, related_obj, relation):
    op = cls()
    
    op.records = ((related_obj, entity, relation), (entity, related_obj, relation.inverse))
    
    def do_relate():
      records = op.records
      for left,right,relation in records:
        if relation is None:
          continue
          
        if relation.cardinality == 'one':
          left.attr[relation.name] = right
        elif relation.cardinality == 'many':
          left.attr[relation.name].values.add(right)
        else:
          raise RuntimeError('Uknonwn relation type {0}'.format(relation.cardinality))
      
      op.records = None

          
    #op.block = do_relate
    op.main = do_relate
    
    return op
  
  def __repr__(self):
    return "<MutableStorage block: %s>" % self.block

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