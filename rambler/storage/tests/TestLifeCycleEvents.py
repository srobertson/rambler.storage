import os


from Rambler import field
from Rambler.TestCase import TestCase

class TestLifeCycleEvents(TestCase):
  componentName = 'Entity'
  test_options = {
    'storage.conf': {'default': 'InMemoryStorage'}
  }
  
  #app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),'../'))
  def setUp(self):
    super(TestLifeCycleEvents,self).setUp()
    # Create a dynamic class on the fly
    self.TestEntity = type('TestEntity', (self.Entity,), {'id': field(str)})
    self.EventService.subscribeToEvent('create', self.on_create, self.Entity)
    self.wait(.1)
    
  def test_listen_for_create(self):
    e = self.TestEntity.create()
    op = e.save()

    self.wait_for(op)
    self.assertEqual(e, self.observed)
    
  def on_create(self, entity):
    self.observed = entity
    
    
