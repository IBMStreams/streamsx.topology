from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import super
from builtins import range
try:
  from future import standard_library
  standard_library.install_aliases()
except (ImportError,NameError):
  # nothing to do here
  pass 
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

import sys 

class TopologyPackages(object):
  def __init__(self): 
    self._conda_packages_to_exclude = set(["numpy", "scipy", "matplotlib", "h5py"])
    self.include_packages = set() 
    self._conda = "Anaconda" in sys.version
    if "Anaconda" in sys.version:
      self.exclude_packages = self._conda_packages_to_exclude
    else: 
      self.exclude_packages = set() 

  def add_excluded_packages(self, packages):
    # place holder for future
    True

  def add_included_packages(self, packages):
    # place holder for future
    True

