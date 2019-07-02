import random
from streamsx.spl import spl

def spl_namespace():
  return 'com.example.bingo'

# A SPL source operator that generates bingo pulls.
@spl.source()
def pull():
  while True:
    pull = random.randint(1,76)
    yield "BINGO"[int(pull / 15)] + str(pull)

