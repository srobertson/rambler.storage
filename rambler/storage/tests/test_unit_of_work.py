from nose.tools import eq_, assert_raises
from Rambler.TestCase import TestCase
from models import Employee

class TestUnitOfWork(TestCase):
  test_options = {
    'storage.conf': {'default': 'InMemoryStorage'}
  }
  
  test_components = {
    'Employee': Employee
  }
  
  
  @TestCase.coroutine
  def test_what(self):
    uow = self.Employee.uow()
    
    # this tracks the entity within this context
    big_bob = uow.realize(self.Employee, {'id': 1, 'name': 'bob'}) 
    assert big_bob.is_clean()
    
    # uow.realize will always return the same entity for the same uncommited 
    # context even if the underlying record is different, this represents
    # what may happen if a the data changes in the underlying storage since
    # the context/transaction began
    big_bob2 = uow.realize(self.Employee, {'id': 1, 'name': 'frank'})
    assert big_bob is big_bob2
    assert big_bob.name == 'bob' 
  
    
    # uow can find objects given a store and a query
    employee = yield uow.find(self.Employee, big_bob.primary_key)
    assert isinstance(employee, self.Employee)
    assert employee is big_bob
    
    
    # find a list of objects, assert they're the exact same even if (according to the storage) 
    # the underlying record data has changed
    count = 0
    for entity in (yield uow.find(self.Employee, 'all')):
      count += 1
      assert entity is big_bob  
    eq_(count,1)
        
        
    
    # unit of work will track newly created entities and merge them with results that match
    bob = self.Employee.create(**{'id':2, 'name': 'bob'})
    assert bob.is_new()

    result = yield uow.find(self.Employee, 2)

    assert bob is result
    
    # uow should barf if asked to create the same entity in the same context
    assert_raises(ValueError,  self.Employee.create, **{'id':2, 'name': 'bobby'})

    # bob is now findable even though it hasn't been commited to the underlying storeage

    count = 0
    for entity in (yield uow.find(self.Employee, 'all', name='bob')):
      count += 1
      if entity.primary_key == 1:
        assert entity is big_bob
      elif entity.primary_key == 2:
        assert entity is bob
      else: # gaurd
        raise RuntimeError('should never reach here')
    eq_(count, 2)
    
       
    # unit of work will track mutations to relations
    bob.set_value_for_key(big_bob, 'manager')
    subordinates = yield big_bob.subordinates()

    #assert big_bob.is_dirty() # you bet he is! heh
    
    # inverse relationships are implictly updated
    
    assert bob in subordinates

    # everything waiting to be flushed
    eq_(uow.changes().all(), [
      {'type':'create', 'object': bob},
      {'type':'relate', 'object':big_bob, 'relation': 'subordinates', 'values': [bob]}
    ])
    
    # this is how you insert a to_many
    # sally = self.Employee.create(id=3, name='sally')
    # big_bob.set_value_for_key(sally, 'subordinates.@add')
    # big_bob.subordinates.add(sally)
    
    # if we modify a new object before it's commited, it's attributes are changed, but
    # the mutation is not neccesarily tracked
    bob.name = 'bobby'
    uow.changes() == [
      {'create': 'Employee', 'mutations': {'id': 2, 'name': 'bobby'}},
      {'relate': 'Employee', 'mutations': {'id': 2, 'manager': 1}},
      {'relate': 'Employee', 'mutations': {'id': 1, 'subordinates.@add': 2}}
    ]
    
    # commiting the uow will flush the changes to the stores
    uow.commit()
    eq_(uow.changes().all(), [])
    
    # The entities  will still be tracked and clean
    # ... note this would query the storage
    for obj in uow.objects(): # only returns entities know about in the context
      assert obj.is_clean()
    
    # be nice to ensure this does not trigger a new round trip to the store  
    bob is uow.find(self.Employee, 2)
    
    # removing an object
    bob.remove()
    assert bob.is_removed()
    # accessing an attribute of a delete object should throw an error
    
    uow.changes() == [
      {'remove': 'Employee', 'mutations': {'id': 2}},
      {'update': 'Employee', 'mutations': {'id': 1, 'subordinates.@remove':2}},
    ]
    
    big_bob.title = 'The one and only'
    
    uow.changes() == [
      {'remove': 'Employee', 'mutations': {'id': 2}},
      {'update': 'Employee', 'mutations': {'id': 1, 'subordinates.@remove':2}},
      {'update': 'Employee', 'mutations': {'id': 1, 'title': 'The one and only'}},
    ]
    
    uow.commit()
    eq_(uow.changes().all(), [])
    assert big_bob.is_clean()
    subordinates = yield big_bob.subordinates()
    
    eq_(len(subordinates), 0)
    
    # forget everything
    uow.clear()
    assert len(uow.objects()) == 0
    
    # refs to entity are now invalid, would be nice to throw an exception
    #assert_raises(ReferenceError, big_bob.value_for, 'name')
    #assert_raises(ReferenceError, lambda o: o.name, big_bob)
    
    # Big Bob wasn't added through the storage so he shouldn't be findable
    # when we clear the uow
    big_bob = yield uow.find(self.Employee, 1)
    assert big_bob is None
    
    
    
    

    