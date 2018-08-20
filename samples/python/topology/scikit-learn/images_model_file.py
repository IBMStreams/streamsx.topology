# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import itertools
import os

from sklearn import datasets, svm
from sklearn.externals import joblib

from streamsx.topology.topology import Topology
import streamsx.topology.context

from digit_predict import DigitPredictor

def main():
    """
    This is a variant of images.py that loads the model from a file.

    Here the Streams application is declared using a model
    contained in a file. This is a typical pattern where
    the model is created off-line and saved to a file.
    Subsequently applications load the file to perform predictions.

    Comments are mainly focused on the model loading, see
    images.py for details on other statements.

    http://scikit-learn.org/stable/modules/model_persistence.html
    """
    # Load the data and train the model.
    digits = datasets.load_digits()
    clf = svm.SVC(gamma=0.001, C=100.)
    clf.fit(digits.data[:-10], digits.target[:-10])

    # Persist the model as a file
    joblib.dump(clf, 'digitmodel.pkl')

    # Just to ensure we are not referencing the local
    # instance of the model, we will load the model at
    # runtime from the file.
    clf = None

    topo = Topology(namespace='ScikitLearn', name='ImagesModelFile')

    topo.add_pip_package('scikit-learn')
    topo.exclude_packages.add('sklearn')

    images = topo.source(itertools.cycle(digits.data[-10:]), name='Images')

    # Add the model to the topology. This will take a copy
    # of the file and make it available when the job
    # is running. The returned path is relative to the
    # job's application directory. See DigitPredictor() for
    # how it is used.
    model_path = topo.add_file_dependency('digitmodel.pkl', 'etc')

    # Predict the digit from the image using the trained model.
    # The map method declares a stream (images_digits) that is
    # the result of applying a function to each tuple on its
    # input stream (images) 
    #
    # At runtime we need to load the model from the file so instead
    # of a stateless lambda function we use an instance a class.
    # This class (DigitPredictor) has the model path as its state
    # and will load the model from the file when the job is excuting
    # in the IBM Cloud.
    images_digits = images.map(DigitPredictor(model_path), name='Predict Digit')

    images_digits.for_each(lambda x : None, name='Noop')

    # Note at this point topo represents the declaration of the
    # streaming application that predicts digits from images.
    # It must be submitted to an execution context, in this case
    # an instance of Streaming Analytics service running on IBM Cloud.

    sr = streamsx.topology.context.submit('STREAMING_ANALYTICS_SERVICE', topo)
    print(sr)

    # Clean up, the running job has its own copy of the model file
    os.remove('digitmodel.pkl')

if __name__ == '__main__':
    main()
