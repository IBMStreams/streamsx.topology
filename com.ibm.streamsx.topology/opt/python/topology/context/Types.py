__author__ = 'wcmarsha'


class Types:
    """Contains the strings values for the different available streams contexts.
    Enums are not supported in python 2.7, so strings are necessary
    """

    TOOLKIT = "TOOLKIT_CONTEXT"
    '''Execution of the topology produces the application as a Streams toolkit.'''

    STANDALONE_BUNDLE = "STANDALONE_BUNDLE_CONTEXT"
    '''Bundles the topology into a standalone executable'''

    STANDALONE = "STANDALONE_CONTEXT"
    '''Bundles the topology into a standalone executable and runs it'''

    DISTRIBUTED_BUNDLE = "DISTRIBUTED_BUNDLE_CONTEXT"
    '''Bundles the topology into a distributed executable'''

    DISTRIBUTED = "DISTRIBUTED_CONTEXT"
    '''Bundles the topology into a distributed executable and submits it to the default instance/domain'''