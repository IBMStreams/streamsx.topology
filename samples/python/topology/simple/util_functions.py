# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import time
import os

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
                file_path = os.path.abspath(os.path.join(self.directory, file))
                if os.path.isfile(file_path):
                    current_files.add(file_path)
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
        try:
            with open(file_name) as f:
                lines = f.read().splitlines()
            all_lines.extend(lines)
        except Exception as e:
            print("Skipping: {0}".format(file_name))
    return all_lines
