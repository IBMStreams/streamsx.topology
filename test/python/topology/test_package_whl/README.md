# tst_package_whl

This is a python package for test purpose only.

Wheel file is created with the following command:

```
cd package/
python3 setup.py bdist_wheel -d ../whl
```

The whl file is required by the test case `test_add_pip_package_whl_from_url` in "test2_pkg.py".
