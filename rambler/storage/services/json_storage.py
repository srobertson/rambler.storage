from Rambler import component, outlet, option
from Rambler.robject import RObject
import json
import re

# converts 'blah' to ('blah', None) or 'blah.@add' to ('blah','@add')
mutation_re = re.compile('(\w+)(?:\.(@(?:\w*)))?')

class JSONStorage(component('InMemoryStorage')):
  JSONCoder = outlet('JSONCoder')
  comp_reg  = outlet('ComponentRegistry')
  location = option('json_storage', 'location', None)
  
  def assembled(self):
    JSONStorage.rebase()
    if isinstance(self.location, basestring):
      self.location = open(self.location, 'w')

  def commit(self, uow):
    # For each change in the change set
    # ensure it's for this given store
    # replace temporary primary keys with new ones
    # save new objects
    # convert relations to set mutations w/ primary key's only
    # save updates
    # save removals

    changes = []
    for change_set in uow.changes():
      #'route type=create to self.handle_create etc..
      handler = getattr(self, 'save_' + change_set['event'], lambda o:None)
      change = handler(change_set)
      if change:
        changes.append(change)
      
    coder = self.JSONCoder(self.location)
    coder.encode_root_object(changes)
    coder.finish_encoding()
    super(JSONStorage, self).commit(uow)
    
  def save_create(self, change_set):
    change_set['model'] = type(change_set['object']).__name__
    return change_set
    
  def save_update(self, change_set):
    o = change_set.pop('object')
    # Todo: THis should be the component name which may differ from the
    # class name
    change_set['model'] = type(o).__name__
    change_set['primary_key'] = o.primary_key
    
    # Convert's object references to primary_keys 
    for key_path, change in change_set['changes'].items():
      if change[RObject.KeyValueChangeKindKey] == RObject.KeyValueChangeInsertion:
        change[RObject.KeyValueChangeNewKey] = [x.primary_key for x in change[RObject.KeyValueChangeNewKey]]
      elif RObject.KeyValueChangeRemoval == 2:
        change[RObject.KeyValueChangeOldKey] = [x.primary_key for x in change[RObject.KeyValueChangeOldKey]]
    
    return change_set
    
  def restore(self, stream):
    for transaction in stream:
      self.replay(transaction)
      
  def replay(self, transaction):
    transaction = json.loads(transaction)
    
    for event in transaction:
      model = self.comp_reg.lookup(event['model'])

      if event['event'] == 'create':
        self.create(model, event)
      elif event['event'] == 'update':
        self.update(model, event)
      elif event['event'] == 'remove':
        self.remove(model,event)
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
    
  def create(self, model, event):
    record = {}
    attrs = event['object']
    self.merge(model, self.pk(model, attrs), attrs, record)
    
  def update(self, model, event):
    primary_key = event['primary_key']
    record =  self.storage_by_class[model][primary_key]
    self.merge(model, primary_key, event['changes'], record)
    
  def remove(self, model, event):
    primary_key = event['primary_key']
    del  self.storage_by_class[model][primary_key]
  
  def merge(self, model, primary_key, mutations, record):
      
    for key, value in mutations.items():
      # key can either be a  key name like attr1 or a set mutation like attr1.@<mutation>
      key,mutation = mutation_re.match(key).groups()
      
      field = model.fields()[key]
      if field.is_relation and field.cardinality == 'many':
        if field.name not in record:
          record[field.name]  = set()
          
        mutation = value[RObject.KeyValueChangeKindKey]
        if mutation == RObject.KeyValueChangeInsertion:
          record[field.name].update(value[RObject.KeyValueChangeNewKey])
        elif mutation == RObject.KeyValueChangeRemoval:
          record[field.name].difference_update(value[RObject.KeyValueChangeOldKey])
      else:        
        record[field.name] = value
    
    self.storage_by_class[model][primary_key] = record
    

  def rollback(self):
    pass
