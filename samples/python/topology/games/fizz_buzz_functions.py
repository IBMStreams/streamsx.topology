# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import time

def int_tuples():
    """
    Generator function that returns a integer count 
    with each iteration (up to 300)
    """
    for count in range(300):
        yield count
        time.sleep(0.1)

def fizz_buzz(tuple):
    """
    Transform the input tuple to a string that follows
    the Fizz Buzz rules
    Args:
        tuple: tuple
    Returns:
        string
    """
    if tuple == 0:
        return None
    
    ret_val = ""
    if tuple % 3 == 0:
        ret_val += "Fizz"
    if tuple % 5 == 0:
        ret_val += "Buzz"
        
    if len(ret_val) == 0:
        ret_val += str(tuple)
    else:
        ret_val += "!"
        
    return ret_val
