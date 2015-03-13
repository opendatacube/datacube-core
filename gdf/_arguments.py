#!/usr/bin/env python

#===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

'''
Created on 12/03/2015

@author: Alex Ip
'''

import sys
import os
import logging
import argparse


from EOtools.utils import log_multiline

# Set top level standard output 
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger = logging.getLogger(__name__)
if not logger.level:
    logger.setLevel(logging.DEBUG) # Default logging level for all modules
    logger.addHandler(console_handler)

class CommandLineArgs(object):
    DEFAULT_ARG_DESCRIPTOR = {'debug': {'short_flag': '-d', 
                                        'long_flag': '--debug', 
                                        'default': False, 
                                        'action': 'store_const', 
                                        'const': True,
                                        'help': 'Debug mode flag'},
                              'config_file': {'short_flag': '-C', 
                                              'long_flag': '--config',
                                              'default': None,
                                              'action': None,
                                              'store_const': False,
                                              'help': 'Configuration file'
                                    }
                    }
    
    def parse_args(self, arg_descriptor_dict):
        """Virtual function to parse command line arguments.
    
        Parameters:
            arg_descriptor_dict: dict keyed by dest variable name containing sub-dicts as follows:
                'short_flag': '-d', 
                'long_flag': '--debug', 
                'default': <Boolean>, 
                'action': 'store_const', 
                'const': <Boolean>,
                'help': <help string>
                
        Returns:
            argparse namespace object
        """
        logger.debug('Calling parse_args()')
            
        _arg_parser = argparse.ArgumentParser(description=os.path.basename(sys.argv[0]))
        
        for dest in sorted(arg_descriptor_dict.keys()):
            _arg_parser.add_argument(arg_descriptor_dict['short_flag'],
                                     arg_descriptor_dict['long_flag'],
                                     dest=dest,
                                     default=arg_descriptor_dict['default'],
                                     action=arg_descriptor_dict['action'],
                                     const=arg_descriptor_dict['const'],
                                     help=arg_descriptor_dict['help']
                                     )
    
        args, _unknown_args = _arg_parser.parse_known_args()
        
        return args
    
    def __init__(self, arg_descriptor_dict=None):
        '''Constructor for class CommandLineArgs
        
        Parameters:
            arg_descriptor_dict: dict keyed by dest variable name containing sub-dicts as follows:
                'short_flag': '-d', 
                'long_flag': '--debug', 
                'default': <Boolean>, 
                'action': 'store_const', 
                'const': <Boolean>,
                'help': <help string>
        '''
        arg_descriptor_dict = arg_descriptor_dict or CommandLineArgs.DEFAULT_ARG_DESCRIPTOR
        self._args = self.parse_args(arg_descriptor_dict)
        
        log_multiline(logger.debug, self.__dict__, 'CommandLineArgs.__dict__', '\t')
        
    @property
    def args(self):
        return self._args
    
