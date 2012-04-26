from Rambler.TestCase import TestCase
import json
from cStringIO import StringIO
from models import Employee
from nose.tools import eq_, assert_raises

data = [
  # Example of an object with no relations
  [{'event':'create', 'type':'Employee', 'record':{'id':1, 'name': 'El Guapo'}}],
  
  # New employee added and related to an existing object in the same transaction
  [
    {'event':'create', 'type':'Employee', 'record':{'id':2, 'name':'El Hefe', 'manager': 1}},
    {'event':'update', 'type':'Employee', 'record':{'id':1, 'subordinates.@add': 2}}
  ],
  # Remove employee 2, this should also nuke the relations, note the storage just
  # replays the events it's not smart enoguh to do the remove without being told
  # by someone eles
  [
    {'event':'remove', 'type':'Employee', 'record':{'id': 2}},
    {'event':'update', 'type':'Employee', 'record':{'id':1, 'subordinates.@remove': 2}}
  ],
  [
    {'event': 'create', 'type':'Employee', 'record':{'id': 3, 'name':'Lucky Day', 'subordinates': [4,5] }},
    {'event': 'create', 'type':'Employee', 'record':{'id': 4, 'name':'Dusty Bottoms', 'manager': 3 }},
    {'event': 'create', 'type':'Employee', 'record':{'id': 5, 'name':'Ned Nederlander', 'manager': 3 }},
  
  ]
  
]

class TestJSONStorage(TestCase):
  test_options = {
    'storage.conf': {'default': 'JSONStorage'}
  }
  
  test_components = {
    'Employee': Employee
  }
  
  
  
  @TestCase.coroutine
  def test_replay(self):
    # ensure storage class is empty
    assert len(self.JSONStorage.storage_by_class) == 0
    
    yield self.JSONStorage.replay(json.dumps(data[0]))
    eq_(len(self.JSONStorage.storage_by_class), 1)
    eq_(len(self.JSONStorage.storage_by_class[self.Employee]), 1)
    
    record = self.JSONStorage.find(self.Employee,1)
    eq_(record['id'], 1)
    eq_(record['name'], 'El Guapo')
    assert 'manager' not in record
    assert 'subordinates' not in record
    
    yield self.JSONStorage.replay(json.dumps(data[1]))
    
    record = yield self.JSONStorage.find(self.Employee,1)
    eq_(record['id'], 1)
    eq_(record['name'], 'El Guapo')
    assert 'manager' not in record
    eq_(len(record['subordinates']), 1)
    assert 2 in record['subordinates']
    
    record = self.JSONStorage.find(self.Employee,2)
    eq_(record['id'], 2)
    eq_(record['name'], 'El Hefe')
    eq_(record['manager'],1)
    assert 'subordinates' not in record
    
    yield self.JSONStorage.replay(json.dumps(data[2]))
    # in transaction 3 we remove El Hefe, so there should only be El Guapo
    eq_(len(self.JSONStorage.storage_by_class[self.Employee]), 1)
    
    record = yield self.JSONStorage.find(self.Employee,1)
    eq_(record['id'], 1)
    eq_(record['name'], 'El Guapo')
    assert 'manager' not in record
    eq_(len(record['subordinates']), 0)
    
    assert_raises(KeyError, self.JSONStorage.find,self.Employee,2)
    
    yield self.JSONStorage.replay(json.dumps(data[3]))
    
    records = yield self.JSONStorage.find(self.Employee, 'all')
    eq_(len(records),4)
    
    ids = []
    for record in records:
      ids.append(record['id'])
      if record['id'] == 1:
        eq_(record['name'], 'El Guapo')
        assert 'manager' not in record
        eq_(len(record['subordinates']), 0)
      elif record['id'] == 3:
        eq_(record['name'], 'Lucky Day')
        assert 'manager' not in record
        eq_(len(record['subordinates']), 2)
        assert isinstance(record['subordinates'], set)
        assert 4 in record['subordinates']
        assert 5 in record['subordinates']
      elif record['id'] == 4:
        eq_(record['name'], 'Dusty Bottoms')
        eq_(record['manager'],3)
        assert 'subordinates' not in record
      elif record['id'] == 5:
        eq_(record['name'], 'Ned Nederlander')
        eq_(record['manager'],3)
        assert 'subordinates' not in record
      
    eq_(sorted(ids), [1,3,4,5])
    
  @TestCase.coroutine
  def test_restore(self):
    stream = StringIO()
    for transaction in data:
      stream.write(json.dumps(transaction))
      stream.write('\n')
    stream.seek(0)
    
    self.JSONStorage.restore(stream)
  
        
    

