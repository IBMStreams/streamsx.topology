# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import os
from sklearn.externals import joblib
import streamsx.ec

class DigitPredictor(object):
    """
    Callable class that loads the model from a file in
    its context manager methods.
    """
    def __init__(self, model_path):
        # Note this method is only called when the topology is
        # declared to create a instance to use in the map function.
        self.model_path = model_path
        self.clf = None

    
    def __call__(self, image):
        """Predict the digit from the image.
        """
        return {'image':image, 'digit':self.clf.predict(image.reshape(1,-1))[0]}
        
    def __enter__(self):
        """Load the model from a file.
        """
        # Called at runtime in the IBM Streams job before
        # this instance starts processing tuples.
        self.clf = joblib.load(
            os.path.join(streamsx.ec.get_application_directory(), self.model_path))

    def __exit__(self, exc_type, exc_value, traceback):
        # __enter__ and __exit__ must both be defined.
        pass
