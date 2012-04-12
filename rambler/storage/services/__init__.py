from Rambler import outlet,load_classes
class ModelLoader(object):
  comp_reg = outlet('ComponentRegistry')
  app = outlet('Application')
  
  def assembled(self):      
      mod_names = ['Rambler', self.app.name] + [ext.name for ext in self.app.config.extensions]
      for mod_name in mod_names:
        mod_full_name = mod_name + ".models"
        for cls in load_classes(mod_full_name, object):
          if hasattr(cls, "provides"):
            name = cls.provides
          else:
            name = cls.__name__

          self.comp_reg.addComponent(name,cls)
            