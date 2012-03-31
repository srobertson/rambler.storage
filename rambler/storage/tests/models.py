from Rambler import component, field

class Client(component('Entity')):
  id   = field(str)
  name = field(str)

  @classmethod
  def assembled(cls):
    cls.rebase()
    cls.has_one('address')

class Address(component('Entity')):
  id     = field(str)
  street = field(str)

  @classmethod
  def assembled(cls):
    cls.rebase()
    cls.belongs_to('client')
  
  

class Employee(component('Entity')):
  id = field(str)
  name = field(str)

  @classmethod
  def assembled(cls):
    cls.rebase()
    # Every employee has one manager
    cls.belongs_to('manager', destination='Employee', inverse='subordinates')
    cls.has_many('subordinates', destination='Employee', inverse='manager')


class Assembly(component('Entity')):
  id = field(str)

  @classmethod
  def assembled(cls):
    cls.rebase()
    cls.has_and_belongs_to_many('parts')


class Part(component('Entity')):
  id = field(str)
  
  @classmethod
  def assembled(cls):
    cls.rebase()
    cls.has_and_belongs_to_many('assemblies')
    
class Node(component('Entity')):
  id = field(str)
  
  @classmethod
  def assembled(cls):
    cls.rebase()
    cls.belongs_to('parent', destination='Node', inverse='children')
    cls.has_many('children', destination='Node', inverse='parent')