import datetime
import os
import re

import json

from dateutil.parser import parse as parse_date
  
from Rambler import outlet, option,coroutine
from Rambler.RunLoop import Stream


units={'KB': 1024,
      'MB': 1024 * 1024}
# Default unit if not specified is MB
units[None] = units['MB']
      
class EntityLogger(object):
  """Listens for Entity create events, logs the data to a file in JSON format
  and  eventually uploads those files to the url specified by the rambler 
  option s3storage.base_url once the file exceeds a certain size 5MB by
  default.
  """
  comp_reg     = outlet('ComponentRegistry')
  Entity       = outlet('Entity')
  EventService = outlet('EventService')
  RunLoop      = outlet('RunLoop')
  scheduler    = outlet('Scheduler')
  log          = outlet('LogService')
  URLRequest   = outlet('URLRequest')

  base_url = None
  
  
  rotate_log_size_option = option('storage','rotate_log_size', 5)
  log_dir = option('storage', 'log_dir', '#{application.path}/log/entities')
  
  date_format = '%Y-%m-%dT%H:%M:%S'
  host_name = option('system','hostname')
  
  def assembled(self):
    self.EventService.subscribeToEvent('Initializing', self.on_init, object)
    self.EventService.registerEvent('log.replayed', self, type(None))

    
    size, unit = re.match('(\d+)\s*(\w\w)*', str(self.rotate_log_size_option)).groups()
    self.rotate_log_size = int(size) * units[unit]

  def on_init(self,txn_id):
    self.scheduler.queue.add_operation(self.init())
    
  @coroutine
  def init(self):
    for log_file in sorted(os.listdir(self.log_dir)):
      log_file = os.path.join(self.log_dir, log_file)
      self.log.info('replaying %s', log_file)
      if os.path.isfile(log_file):
        for event in open(log_file):
          event = json.loads(event)
          
          entity_cls = self.comp_reg.lookup(event['type'])
          
          # TODO: find a better way to serialize/deserialize json
          record = event['record']
          for key,val in record.items():
            if key == 'date' or key.endswith('_at'):
              record[key] = parse_date(val)
              
          if event['event'] == 'create':
            yield entity_cls.create(**record)
          elif event['event'] == 'relate':

            entity = yield entity_cls.find(record['id'])
            # find the other ojbect
            relation = getattr(entity_cls, record['relation'])
            other  = yield relation.destination.find(record['$ref'])
            yield entity.relate(other, relation)
            
          else:
            # todo use the primary key
            record = event['record']
            entity = yield entity_cls.find(record['id'])
            
            entity.set_values(record)
            yield entity.save()
        

        self.EventService.publishEvent('log.replayed',self,None)
    
    self.log_file = self.create_log_file()
    self.EventService.subscribeToEvent('create', self.on_create, object)
    self.EventService.subscribeToEvent('update', self.on_update, object)
    #self.EventService.subscribeToEvent('delete', self.on_update, object)
    self.EventService.subscribeToEvent('relate', self.on_relate, object)
    
  def create_log_file(self):
    base_path = self.log_dir #os.path.join(self.log_dir, cls.__name__.lower())
    
    log_path = os.path.join(base_path,
                            datetime.datetime.utcnow().strftime(self.date_format))
    log_path = os.path.join(base_path, 'storage.log')
    try:
      return open(log_path, 'a+')
    except IOError,e:
      # make the directory if it's missing
      os.makedirs(base_path)
      return open(log_path, 'a+')
  
  def on_create(self, entity):
    self.record = {}
    entity.encode_with(self)

    self.log_event('create', entity.__class__.__name__, self.record)

  def on_update(self, entity):
    self.record = {}
    entity.encode_with(self)
    self.log_event('update', entity.__class__.__name__, self.record)
    
  def on_relate(self, relation_tuple):
    entity,other,relation = relation_tuple
    # writes {'id': 1234, '$ref': 'abc', 'relation': 'attr1'}
    #import pdb; pdb.set_trace()
    self.log_event('relate', entity.__class__.__name__, {'id': entity.primary_key, '$ref': other.primary_key, 'relation': relation.name})

    pass
  

    
  def log_event(self, event, type, record):
    log_file = self.log_file
    event = {'event': event, 'type':type, 'record': record}
    log_file.write(json.dumps(event))
    log_file.write('\n')
    if log_file.tell() > (self.rotate_log_size):
      self.rotate(entity_type)
        
  def rotate(self,cls):
    return
    log_file = self.log_files_by_class.pop(cls)
    self.upload_file(log_file)
    
  def rotate_logs(self):
    """Rotate all logs that have at least one byte written to them.
    
    Typically invoked when rotate_timeout has been reached.
    """
    if self.log_file.tell() > 0:
      log_file = self.log_file
      self.log_file = self.create_log_file()
      self.upload_file(log_file)

  def encode_int_for(self, i, key):
    self.record[key] = i
    
  def encode_str_for(self, string, key):
    self.record[key] = string
    
  def encode_bool_for(self, bool, key):
    self.record[key] = bool
    
  def encode_datetime_for(self, dt, key):
    if dt:
      self.record[key] = dt.isoformat()
  
  def encode_object_for(self, object, key):
    if isinstance(object, self.Entity):
      assert object._is_new == False
      self.record[key] = {'$ref': object.__name__, '$id': object.primary_key}
    else:
      # Hope the object is json encodeable
      self.record[key] = object
          
  def encode_set_for(self, set, key):
    self.record[key] = tuple(set)
    