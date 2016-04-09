def int_strings_transform():
    return ["325", "457", "9325"]
    
def string_to_int(t):
    return int(t)

class AddNum:
    def __init__(self, increment):
        self.increment = increment  
    def __call__(self, tuple):
        return tuple + self.increment
