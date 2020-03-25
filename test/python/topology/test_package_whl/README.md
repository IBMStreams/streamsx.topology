# test_package_whl

This is a python package for test purpose only.

The whl file `tstexamplepkg-1.0-py3-none-any.whl` in "whl" directory is required by the test case `test_add_pip_package_whl_from_url` in "test2_pkg.py".

## Command to create the whl file:

```
cd package/
python3 setup.py bdist_wheel -d ../whl
```

