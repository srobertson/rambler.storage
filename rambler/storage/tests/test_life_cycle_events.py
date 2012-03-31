from nose.tools import eq_
import os

from Rambler import field, coroutine
from Rambler.TestCase import TestCase

from models import Employee

class TestLifeCycleEvents(TestCase):
  componentName = 'Entity'
  test_options = {
    'storage.conf': {'default': 'InMemoryStorage'}
  }
  
  test_components = {
    'Employee': Employee
  }
  
  def setUp(self):
    super(TestLifeCycleEvents,self).setUp()
    # Create a dynamic class on the fly
    self.TestEntity = type('TestEntity', (self.Entity,), {'id': field(str)})
    self.EventService.subscribeToEvent('create', self.on_create, self.Entity)
    self.wait(.1)
    
  def test_listen_for_create(self):
    def routine():
      e = yield self.TestEntity.create()
    op = self.CoroutineOperation(routine(),self.queue)
    self.wait_for(op)
    self.assertEqual(op.result, self.observed)
    
  def on_create(self, entity):
    self.observed = entity
    
  def test_commit(self):
    
    def routine():

      manager = yield self.Employee.create(name="El Guapo")
      # .. Q: is create now an op?
      uow = coroutine.context.rambler_storage_uow
    
      eq_(len(uow.objects()), 1)
      eq_(len(uow.get_new()), 1)

      yield self.Employee.commit()

      # Commiting the transaction flushes the changes to the storage
      # the unit of work will keep track of the object now in the "clean"
      # state

      eq_(len(uow.objects()), 1)
      eq_(len(uow.get_new()), 0)
      eq_(len(uow.get_clean()), 1)
    
      minion1 = yield self.Employee.create(name="El Hefe", manager=manager)
      eq_(minion1.manager, manager)
      assert minion1 in manager.subordinates
    
      eq_(len(uow.objects()), 2)
      eq_(len(uow.get_new()), 1)
      eq_(len(uow.get_clean()), 1)
    
      # forget throws an error if their are changed objects
      assert_raises(RuntimeError, uow.forget)
    
      yield self.Employee.commit()
    
      eq_(len(uow.objects()), 2)
      eq_(len(uow.get_new()), 0)
      eq_(len(uow.get_clean()), 2)

      uow.forget()
      eq_(len(uow.objects()), 0)
      eq_(len(uow.get_new()), 0)
      eq_(len(uow.get_clean()), 0)
    
    op = self.CoroutineOperation(routine(),self.queue)
    self.wait_for(op)

    op.result

