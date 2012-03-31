import os
import tempfile

from Rambler.TestCase import TestCase
from nose.tools import eq_

from models import *
  

class TestStorage(TestCase):
  test_options = {
    'storage.conf': {'default': 'MutableStorage'},
  }
    
  test_components = {
    'Assembly': Assembly,
    'Part': Part,
    'Employee': Employee,
    'Client': Client,
    'Address': Address,
    'Node': Node
  }
    
  def test_associate(self):
    def routine():
      client = yield Client.create(name='John')
      address = yield Address.create(street='666 Mocking Bird Ln')
    
      client.address = address
      address_after_set = yield client.address()
    
      eq_(address_after_set, address)

      store = client.store
      assert client in store.storage_by_class[Client].values()
      assert address in store.storage_by_class[Address].values()
    op = self.CoroutineOperation(routine(),self.queue)
    self.wait_for(op)
    
    
    
    