import logging

def initialize_logging():
    # create logger with 'streamsx.topology'
    logger = logging.getLogger('streamsx.topology')
    logger.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch.setFormatter(formatter)

    # add the handler to the logger
    logger.addHandler(ch)

    # create file handler which logs even debug messages
    #fh = logging.FileHandler('streamsx.topology.log')
    #fh.setLevel(logging.DEBUG)
    #fh.setFormatter(formatter)
    #logger.addHandler(fh)