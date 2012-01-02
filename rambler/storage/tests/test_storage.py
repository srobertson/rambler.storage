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
    client = self.wait_for(Client.create(name='John'))
    
    address = self.wait_for(Address.create(street='666 Mocking Bird Ln'))
    
    #self.wait_for(client.save())
    #self.wait_for(address.save())
    
    self.wait_for(client.address.set(address))
    address_after_set = self.wait_for(client.address())
    
    eq_(address_after_set, address)

    store = client.store
    assert client in store.storage_by_class[Client].values()
    assert address in store.storage_by_class[Address].values()
    
    
    