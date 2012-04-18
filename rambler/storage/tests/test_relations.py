import os
import tempfile

from Rambler.TestCase import TestCase

from nose.tools import eq_

from models import *
  
  
def coroutine(func):
  def test_coroutine(self):
    op = self.CoroutineOperation(func(self), self.queue)
    self.wait_for(op)

  return test_coroutine

class TestRelations(TestCase):

  test_options = {
    'storage.conf': {'default': 'InMemoryStorage'},
    
  }
  
  test_components = {
    'Assembly': Assembly,
    'Part': Part,
    'Employee': Employee,
    'Client': Client,
    'Address': Address,
    'Node': Node
  }
  
  def setUp(self):
    self.test_options['storage.log_dir'] = tempfile.mkdtemp('rambler.storage')
    super(TestRelations, self).setUp()
    self.publishAppEvent('Initializing','')
    
  def test_has_one_and_belongs_to(self):
    #self.Address.client.association

    role = self.Address.client
    eq_(role.name, 'client')
    eq_(role.destination, Client)
    eq_(role.cardinality, 'one')

    role = role.inverse
    eq_(role.name, 'address')
    eq_(role.destination, Address)
    eq_(role.cardinality, 'one')
    
    
    # The exact opposite of the previous tests 
    role = self.Client.address
    
    eq_(role.name, 'address')
    eq_(role.destination, Address)
    eq_(role.cardinality, 'one')
    
    role = role.inverse
    eq_(role.name, 'client')
    eq_(role.destination, Client)
    eq_(role.cardinality, 'one')

    assert self.Client.address.inverse is self.Address.client
    assert self.Address.client.inverse is self.Client.address
    

  @TestCase.coroutine
  def test_has_many_belongs_to(self):
    role = self.Employee.manager

    # Roles are always returned sorted on name
    eq_(role.name,  'manager')
    eq_(role.destination, Employee)
    eq_(role.cardinality, 'one')

    role = role.inverse
    eq_(role.name,  'subordinates')
    eq_(role.destination, Employee)
    eq_(role.cardinality, 'many')
    
    manager = yield self.Employee.create()
    subs = yield manager.subordinates()
    eq_(len(subs),0)
   
    employee = yield self.Employee.create()
    manager.subordinates.add(employee)
    
    # new employee is now in the collection of the manager's subordinates
    eq_(len(subs),1)
    emp_manager = yield employee.manager()
    eq_(manager, emp_manager)
    
    # verify relation shortcuts work properly
    employee2 = yield manager.subordinates.create()
    
    eq_(len(subs),2)
    #Employee 1's relations should remain the same
    emp_manager = yield employee.manager()
    eq_(manager, emp_manager)
    #Employee #2 should also share the same relation
    emp_manager = yield employee2.manager()
    eq_(manager, emp_manager)
    
    
  def test_has_and_belongs_to_many(self):
    role = self.Part.assemblies

    eq_(role.name,  'assemblies')
    eq_(role.destination, Assembly)
    eq_(role.cardinality, 'many')
    
    role = role.inverse
    eq_(role.name,  'parts')
    eq_(role.destination, Part)
    eq_(role.cardinality, 'many')
    
  def test_self_join(self):
    role = self.Node.parent

    eq_(role.name,  'parent')
    eq_(role.destination, Node)
    eq_(role.cardinality, 'one')
    
    role = role.inverse
    eq_(role.name,  'children')
    eq_(role.destination, Node)
    eq_(role.cardinality, 'many')
    
    