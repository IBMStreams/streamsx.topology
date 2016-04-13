import time
import os
import re
import logging

logging.basicConfig(level = logging.INFO, format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class DirectoryWatcher:
    """
    The class periodically polls the specified directory for files.
    For each file, its contents are read as a text file, producing a 
    tuple for each line of the file. The tuple contains
    the contents of the line, as a string.
    """
    def __init__(self, directory):
        self.directory = directory
        self.files = set()
    def __call__(self):
        """
        Poll the specified directory 3 times, sleeping 10 seconds
        in between each poll.
        """
        for count in range(3):
            current_files = set()
            # get a list of files currently in the directory
            for file in os.listdir(self.directory):
                current_files.add(os.path.abspath(os.path.join(self.directory, file)))
            # get a list of new files
            new_files = current_files.difference(self.files)
            # read content of new files
            lines = _read_files(new_files)
            # return each line as a tuple
            for line in lines:
                yield line
            self.files.update(new_files)
            time.sleep(10)
            
def _read_files(file_names):
    """
    Reads content from all specified file names
    Args:
        file_names: set of file names
    Returns:
        list of lines from all files
    """
    all_lines = []
    for file_name in file_names:
        with open(file_name) as f:
            lines = f.read().splitlines()
        all_lines.extend(lines)
    return all_lines
                
class FilterLine:
    """
    Callable class that returns True if the tuple matches the pattern,
    False otherwise
    Args:
        term (string): search term
    Returns:
        True if the tuple matches the pattern, False otherwise
    """
    def __init__(self, pattern):
        self.pattern = pattern
        self.count = 0
    def __call__(self, tuple):
        self.count += 1
        logger.info("FilterLine@{0} has received {1} lines on this parallel channel.".format(id(self), self.count))
        return re.search(self.pattern,  tuple)

class LineCounter:
    """
    Callable class that counts the number of tuples seen
    """
    def __init__(self):
        self.count = 0
    def __call__(self, tuple):
        self.count += 1
        logger.info("LineCounter@{0} has sent {1} lines to be filtered.".format(id(self), self.count))
        return tuple


        
