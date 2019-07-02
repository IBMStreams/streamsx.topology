import random
from streamsx.spl import spl

def spl_namespace():
  return 'com.example.cards'

# A SPL source operator deals decks of cards.  Each deck is shuffled and then
# dealt completely.  
@spl.source()
def deal():
    deck = list(range(0,52))
    while True:
      random.shuffle(deck)
      for card in deck:
        suit = int(card / 13)
        value = card % 13
        suits = ['spades','hearts','clubs','diamonds']
        values = ['ace','deuce','three','four','five','six','seven','eight','nine','ten','jack','queen','king']
        yield 'the ' + values [value] + ' of ' + suits [suit]

    
