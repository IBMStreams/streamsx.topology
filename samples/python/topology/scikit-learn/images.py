# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import itertools
from sklearn import datasets, svm
from streamsx.topology.topology import Topology
import streamsx.topology.context

def main():
    """
    Introduction to streaming with scikit-learn.

    Adapts the scikit-learn basic tutorial to
    a streaming environment.

    In a streaming environment events arrive continually
    and as individual items. In this case the digit prediction
    example is adapted to predict a digit as each image arrives.

    The training of the prediction model occurs locally using
    the example digits dataset, while the runtime prediction
    of images occurs in the IBM Cloud using the Streaming
    Analytics service.

    The original scikit-learn tutorial is at:
    http://scikit-learn.org/stable/tutorial/basic/tutorial.html 
    """
    # Load the data and train the model.
    digits = datasets.load_digits()
    clf = svm.SVC(gamma=0.001, C=100.)
    clf.fit(digits.data[:-10], digits.target[:-10])

    # Start the streaming application definition
    topo = Topology(namespace='ScikitLearn', name='Images')

    # For use on the service we need to require scikit-learn
    topo.add_pip_package('scikit-learn')
    topo.exclude_packages.add('sklearn')

    # Create a stream of images by cycling through the last
    # ten images (which were excluded from the training)
    # Each tuple on the stream represents a single image.
    images = topo.source(itertools.cycle(digits.data[-10:]), name='Images')

    # Predict the digit from the image using the trained model.
    # The map method declares a stream (images_digits) that is
    # the result of applying a function to each tuple on its
    # input stream (images) 
    #
    # In this case the function is a lambda that predicts the
    # digit for an image using the model clf. Each return
    # from the lambda becomes a tuple on images_digits,
    # in this case a dictionary containing the image and the prediction.
    #
    # Note that the lambda function captures the model (clf)
    # and it will be pickled (using dill) to allow it to
    # be used on the service (which runs in IBM Cloud).
    # 
    images_digits = images.map(lambda image : {'image':image, 'digit':clf.predict(image.reshape(1,-1))[0]}, name='Predict Digit')

    images_digits.for_each(lambda x : None, name='Noop')

    # Note at this point topo represents the declaration of the
    # streaming application that predicts digits from images.
    # It must be submitted to an execution context, in this case
    # an instance of Streaming Analytics service running on IBM Cloud.

    sr = streamsx.topology.context.submit('STREAMING_ANALYTICS_SERVICE', topo)
    print(sr)

if __name__ == '__main__':
    main()
