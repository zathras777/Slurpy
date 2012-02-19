#! /usr/bin/env python

import os
import sys

import argparse

from slurpy import Slurpy
from slurpy.catalog import Catalog2

catalog_filename = ''
drop_tables = False
config_filename = ''

def parse_args():
    parser = argparse.ArgumentParser(description='Command line options:')
    parser.add_argument('catalog_file', nargs='?',
                                        help='Catalog file to use')
    parser.add_argument('--import', action='store_const', const=True,
                        default=False, dest='import', help='Import data')
    parser.add_argument('--drop-tables', action='store_const', const=False,
                        default=True, dest='drop_tables',
                                                    help='Drop all tables')
    parser.add_argument('--config-file', action='store', default = '',
                                       help='Accumulate data from clients')

    return parser.parse_args().__dict__

def find_config(set_path):
    ''' Look through a set of directories looking for the slurpy.conf
        file. '''
    if set_path != '':
        if os.path.exists(set_path):
            return set_path
        return ''
    dirs = [ os.path.expanduser("~/.slurpy"), os.path.realpath(".") ]
    for d in dirs:
        cfn = os.path.join(d, 'slurpy.conf')
        if os.path.exists(cfn):
            print "Found config file: %s" % cfn
            return cfn
    return ''

def process_catalog():   
    if not catalog_filename:
        print "You must supply a catalog filename to process."
        sys.exit(0)
        
    c = Catalog2(catalog_filename)       
    s = Slurpy()
    if not s.read_config_file(config_filename):
        print "Unable to read the config file %s" % config_filename
        sys.exit(0)
    
    if not s.connect():
        print "Unable to connect to the slurpy database :-("
        sys.exit(0)

    # Now we are connected, check that the pg database has the tables we
    # need. If the --drop-tables flag was given, then we drop all tables
    # and create them anew.
    if not s.check_schema(c, drop_tables):
        print "Schema check failed."
        sys.exit(0)
   
    s.import_catalog(c)

    for t in tables:
        a = c.get_table_row_count(t)
        b = s.get_table_row_count(t)
        if a != b:
            print "%-40s   %s vs %s" % (t, a, b)

        
    s.close()
    
def command_line():
    cfg = parse_args()
    drop_tables = cfg.get('drop_tables')
    catalog_filename = cfg.get('catalog_file', '')

    config_filename = find_config(cfg.get('config_file', ''))
    if config_filename == '':
        print "Unable to find a config file..."
        sys.exit(0)
        
    process_catalog()

