__author__ = 'wcmarsha'


class ContextProperties:
    """Properties that can be specified when submitting the topology to a context."""

    TOOLKIT_DIR = "topology.toolkitDir"
    '''Location of the generated toolkit root.
     If not supplied to the configuration passed to a context's submit() method,
     a unique location will be used, and placed into the configuration with this key.
     The value should be a {@code String} that is the absolute path of the toolkit directory.
     '''