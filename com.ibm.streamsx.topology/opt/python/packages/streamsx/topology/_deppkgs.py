# coding=utf-8
# Packages streamsx depends on and thus must be installed
# These packages are not added into the Streams application.

_DEP_PACKAGES = [
    "dill",
    "enum34",
    "requests"
 ]

# Notebook packages to be excluded when running in ICP4D
_ICP4D_NB_PACKAGES = [
     'pyspark',
     'icpd_core',
 ]
