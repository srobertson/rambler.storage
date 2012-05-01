from nose.tools import eq_, assert_raises
from Rambler.TestCase import TestCase
from models import Employee

import json

class TestUnitOfWork(TestCase):
  test_options = {
    'storage.conf': {'default': 'InMemoryStorage'}
  }
  
  test_components = {
    'Employee': Employee
  }
  
  @TestCase.coroutine
  def test_encode(self):
    coder = self.JSONCoder()
    emp = yield self.Employee.create(name='Bob', title='CEO')
    coder.encode_root_object([1,2,{}, emp])
    
    coder.finish_encoding()
    self.assertSequenceEqual(
      json.loads(coder.buffer.getvalue()),
      [1,2,{},{'name':'Bob', 'title':'CEO', 'id': emp.id}]
    )
  
  @TestCase.coroutine
  def test_robject_as_suboject(self):

    emp = yield self.Employee.create(name='Bob', title='CEO')
    coder = self.JSONCoder()
    coder.encode_root_object({'subkey': emp})

    coder.finish_encoding()
    coded = json.loads(coder.buffer.getvalue())

    self.assertDictEqual(
      coded,
      {'subkey':{'name':'Bob', 'title':'CEO', 'id': emp.id}}
    )
   
  @TestCase.coroutine  
  def test_embeded_dict_with_embeded_robject_inside_list(self):
    emp = yield self.Employee.create(name='Bob', title='CEO')
    coder = self.JSONCoder()
    coder.encode_root_object([{'subkey': emp}])

    coder.finish_encoding()
    coded = json.loads(coder.buffer.getvalue())

    self.assertSequenceEqual(
      coded,
      [{'subkey': {'name':'Bob', 'title':'CEO', 'id': emp.id}}]
    )

  @TestCase.coroutine  
  def test_embeded_list_with_embeded_robject_inside_list(self):
    emp = yield self.Employee.create(name='Bob', title='CEO')
    coder = self.JSONCoder()
    coder.encode_root_object([[emp]])

    coder.finish_encoding()
    coded = json.loads(coder.buffer.getvalue())

    self.assertSequenceEqual(
      coded,
      [[{'name':'Bob', 'title':'CEO', 'id': emp.id}]]
    )
  
    
    
  @TestCase.coroutine  
  def test_embeded_list_with_embeded_robject_inside_dict(self):
    emp = yield self.Employee.create(name='Bob', title='CEO')
    coder = self.JSONCoder()
    coder.encode_root_object({'mylist': [emp]})

    coder.finish_encoding()
    coded = json.loads(coder.buffer.getvalue())

    self.assertDictEqual(
      coded,
      {'mylist': [{'name':'Bob', 'title':'CEO', 'id': emp.id}]}
    )