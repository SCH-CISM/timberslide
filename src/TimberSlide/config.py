'''
Created on 23/12/2014

@author: asieira
'''
from ConfigParser import SafeConfigParser

def load_config(filename = None):
    parser = SafeConfigParser({'default': True})
    if not filename == None:
        parser.read(filename)
    return parser

if __name__ == "__main__":
    config = load_config()
    print "Defaults:"
    print config.defaults() 
    for section in config.sections():
        print "section " + section + ":"
        print config.options(section)