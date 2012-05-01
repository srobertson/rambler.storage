from Rambler.TestCase import TestCase
import json
from cStringIO import StringIO
from models import Employee
from nose.tools import eq_, assert_raises


updates = [
  # Example of an object with no relations
  [{'event': 'create', 'model': 'Employee', 'object': {'id':1, 'name': 'El Guapo', 'title': None}}],
  
  # Creates a new object relating it to the previous
  [
    {'event': 'create', 'model': 'Employee', 'object': {'id':2, 'name': 'El Hefe', 'title': None, 'manager':1}},
    {'event': 'update', 'model': 'Employee', 'primary_key':1, 'changes': {
      'subordinates': {'KeyValueChangeKindKey': 2,'KeyValueChangeNewKey': [2]}}
    }
  ],
  
  # Remove a previous object
  [
    {'event': 'remove', 'model':'Employee', 'primary_key': 2},
    {'event': 'update', 'model': 'Employee', 'primary_key':1, 'changes': {
        'subordinates': {'KeyValueChangeKindKey':3,'KeyValueChangeOldKey': [2]}
      }
    }
  ]
]


class TestJSONStorage(TestCase):
  test_options = {
    'storage.conf': {'default': 'JSONStorage'},
   
  }
  
  test_components = {
    'Employee': Employee
  }
  
  @TestCase.coroutine
  def test_create(self):
    stream = self.test_options['json_storage.location'] = StringIO()
    
    assert len(self.JSONStorage.storage_by_class) == 0
    el_guapo = yield self.Employee.create(id=1, name="El Guapo")
    assert len(self.JSONStorage.storage_by_class) == 0
    yield self.Employee.commit()
    assert len(self.JSONStorage.storage_by_class) == 1
    assert len(self.JSONStorage.storage_by_class[self.Employee]) == 1
    
    hefe = yield self.Employee.create(id=2, name="El Hefe", manager=el_guapo)
    yield self.Employee.commit()
    
    eq_(len(self.JSONStorage.storage_by_class),1)
    eq_(len(self.JSONStorage.storage_by_class[self.Employee]),2)
    
    stream.seek(0)
    record = json.loads(stream.readline())
    self.assertSequenceEqual(updates[0], record)

    line = stream.readline()
    record = json.loads(line)
    self.assertItemsEqual(updates[1], record )
    
    
  
  @TestCase.coroutine
  def test_replay(self):
    # ensure storage class is empty
    assert len(self.JSONStorage.storage_by_class) == 0
    
    yield self.JSONStorage.replay(json.dumps(updates[0]))
    eq_(len(self.JSONStorage.storage_by_class), 1)
    eq_(len(self.JSONStorage.storage_by_class[self.Employee]), 1)
    
    record = self.JSONStorage.find(self.Employee,1)
    eq_(record['id'], 1)
    eq_(record['name'], 'El Guapo')
    assert 'manager' not in record
    assert 'subordinates' not in record
    
    yield self.JSONStorage.replay(json.dumps(updates[1]))
    
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
    
    yield self.JSONStorage.replay(json.dumps(updates[2]))
    # in transaction 3 we remove El Hefe, so there should only be El Guapo
    eq_(len(self.JSONStorage.storage_by_class[self.Employee]), 1)
    
    record = yield self.JSONStorage.find(self.Employee,1)
    eq_(record['id'], 1)
    eq_(record['name'], 'El Guapo')
    assert 'manager' not in record
    eq_(len(record['subordinates']), 0)
    
    assert_raises(KeyError, self.JSONStorage.find,self.Employee,2)
    
    
    # TODO: adding multiple objects with a to-many relationship in the same transaction 
    return
    yield self.JSONStorage.replay(json.dumps(updates[3]))
    
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
    for transaction in updates:
      stream.write(json.dumps(transaction))
      stream.write('\n')
    stream.seek(0)
    
    self.JSONStorage.restore(stream)
  
        
    

