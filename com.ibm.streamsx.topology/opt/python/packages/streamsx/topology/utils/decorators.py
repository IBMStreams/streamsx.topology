# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

def overrides(interface_class):
    def overrider(method):
        if not method.__name__ in dir(interface_class):
            raise Exception("Method override failed. Method " + method.__name__ + " is not present in " +
                            interface_class.__name__)
        method.__doc__ = getattr(interface_class, method.__name__)
        return method
    return overrider
