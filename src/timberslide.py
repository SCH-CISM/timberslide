#!/usr/local/bin/python2.7
# encoding: utf-8
'''
timberslide -- S3 to PostgreSQL batch load utility for Niddel-generated logs

@author:     https://github.com/asieira

@copyright:  2014 Niddel Corp and Seattle Children's Hospital. All rights reserved.

@contact:    contact@niddel.com
@deffield    updated: Updated
'''

import sys
import os

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from TimberSlide.slots import parseSlot, mergeSlotSets
from test.test_support import args_from_interpreter_flags
from sys import argv
from TimberSlide.s3repository import S3Repository

__all__ = []
__version__ = 0.1
__date__ = '2014-12-23'
__updated__ = '2014-12-23'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by https://github.com/asieira on %s.
  Copyright 2014 Niddel Corp. and Seattle Children's Hospital. All rights reserved.

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        #parser.add_argument("-c", "--config", dest="config", help="path to the configuration file [default: %(default)s]", default="~/.timberslide")
        parser.add_argument('-v', '--version', action='version', version=program_version_message)
        parser.add_argument('-r', '--repository', help='S3 directory where the data is located', default='s3://nevermind-logs/export/')
        parser.add_argument('slot', nargs='+', help='time slots or ranges of time slots to load, either <slot> or <slot>-<slot> for an inclusive range, <slot>- for all slots above and -<slot> for all slots below the provided one; each slot should be in YYYY, YYYYMM, YYYYMMDD or YYYYMMDDHH format (UTC)')

        # Process arguments
        args = parser.parse_args()
        
        # merge slots and give feedback
        args.repository = S3Repository(args.repository)
        args.slot = mergeSlotSets([parseSlot(s, args.repository) for s in args.slot])
        print "Slots to process: "
        print "\t" + ", ".join(sorted([str(s) for s in args.slot], reverse=True))
        print "Files to process:"
        for k in args.repository.slotkeys(args.slot):
            print "\ts3://" + args.repository.bucket + '/' + k.name

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
#     if DEBUG:
#         sys.argv.append("-h")
#         sys.argv.append("-v")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'timberslide_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())