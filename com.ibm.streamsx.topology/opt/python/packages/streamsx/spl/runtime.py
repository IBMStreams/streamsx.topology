#
# Wrap the operator's iterable in a function
# that when called returns each value from
# the iteration returned by iter(callable).
# It the iteration returns None then that
# value is skipped (i.e. no tuple will be
# generated). When the iteration stops
# the wrapper function returns None.
#
def _splpy_iter_source(iterable) :
  it = iter(iterable)
  def _wf():
     try:
        while True:
            tv = next(it)
            if tv is not None:
                return tv
     except StopIteration:
       return None
  return _wf
