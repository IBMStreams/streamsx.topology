import time 

class customsource:
    def __call__(self):
        i = 0
        while i < 2:
            yield "hi"
            time.sleep(1)
            i = i + 1
