from Rambler import component,outlet
import json
import re

# converts 'blah' to ('blah', None) or 'blah.@add' to ('blah','@add')
mutation_re = re.compile('(\w+)(?:\.(@(?:\w*)))?')

class JSONStorage(component('InMemoryStorage')):
  JSONCoder = outlet('JSONCoder')
  comp_reg  = outlet('ComponentRegistry')
  

  def commit(self, uow):
    coder = self.JSONCoder()
    # # iterate through objects in order they were inserted into the uow
    # for obj in uow.where(store=self):
    #   if obj['state'] == uow.REMOVED:
    #      del self.storage_by_class[type(obj)][obj.primary_key]
    #      coder.encode_root_object({'event': 'remove', (obj.__class__.__name__, obj.primary_key)})
    #      
    #   elif obj['state'] == uow.RELATE:
    #     coder.encode_root_object({'event': 'relate', 'record':obj.obj})
    #   elif obj['state'] == uow.UNRELATE:
    #     coder.encode_root_object({'event': 'unrelate', 'record':obj.obj})
    #   elif obj['state'] == uow.DIRTY:
    #     coder.encode_root_object({'event': 'update', 'record':obj.changes})
    #   elif obj['state'] == uow.NEW:
    #     coder.encode_root_object({'event': 'create', 'record':obj.attr})
    #     
        

    for obj in uow.where(store=self, state=uow.NEW):
      coder.encode_root_object(obj)
      #obj.encode_with(coder)
      self.storage_by_class[type(obj)][obj.primary_key] = obj.attr.copy()
    
    for obj in uow.where(store=self, state=uow.DIRTY):
      coder.encode_root_object(obj)
      self.storage_by_class[type(obj)][obj.primary_key] = obj.attr.copy()
    
    for obj in uow.where(store=self, state=uow.REMOVED):
      del self.storage_by_class[type(obj)][obj.primary_key]
    
    coder.finish_encoding()
    
    print "-->", coder.buffer.getvalue()

  def restore(self, stream):
    for transaction in stream:
      self.replay(transaction)
      
  def replay(self, transaction):
    transaction = json.loads(transaction)
    
    for event in transaction:

      model = self.comp_reg.lookup(event['type'])
      record = event['record']
  
      if event['event'] == 'create':
        self.create(model,record)
      elif event['event'] == 'update':
        self.update(model,record)
      elif event['event'] == 'remove':
        self.remove(model,record)
      else:
        raise RuntimeError('Uknown event {0}'.format(event['event']))


  def pk(self, model, mutations):
    primary_key = []
    for field in model.primary_key_fields:
      primary_key.append(mutations[field])
    if len(primary_key) == 1:
      primary_key = primary_key[0]
    else:
      primary_key = tuple(primary_key)
    return primary_key
    
  def create(self, model, mutations):
    record = {}
    self.merge(model, self.pk(model, mutations), mutations, record)
    
  def update(self, model, mutations):
    primary_key = self.pk(model, mutations)
    record =  self.storage_by_class[model][primary_key]
    self.merge(model, primary_key, mutations, record)
    
  def remove(self, model, mutations):
    primary_key = self.pk(model, mutations)
    del  self.storage_by_class[model][primary_key]
  
  def merge(self, model, primary_key, mutations, record):
      
    for key, value in mutations.items():
      # key can either be a  key name like attr1 or a set mutation like attr1.@<mutation>
      key,mutation = mutation_re.match(key).groups()
      
      field = model.fields()[key]
      if field.is_relation and field.cardinality == 'many' and field.name not in record:
        record[field.name]  = set()
      
      if mutation == '@add':
        record[field.name].add(value)
      elif mutation == '@remove':
        record[field.name].remove(value)
      elif mutation is None:
        if field.is_relation and field.cardinality == 'many':
          value = set(value)
        record[field.name] = value
      else:
        raise RuntimeError('Unkown mutation {0}'.format(mutation))
  
    
    self.storage_by_class[model][primary_key] = record
    

  def rollback(self):
    pass
