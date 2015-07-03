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


from eotools.utils import log_multiline

logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG) # Logging level for this module

class CommandLineArgs(object):
    DEFAULT_ARG_DESCRIPTORS = {'debug': {'short_flag': '-d', 
                                        'long_flag': '--debug', 
                                        'default': False, 
                                        'action': 'store_const', 
                                        'const': True,
                                        'help': 'Debug mode flag'
                                        },
                              'config_files': {'short_flag': '-C', 
                                              'long_flag': '--config',
                                              'default': None,
                                              'action': 'store',
                                              'const': None,
                                              'help': 'Comma-delimited list of configuration files'
                                    }
                    }
    
    def _parse_args(self, arg_descriptors):
        """Virtual function to parse command line arguments.
    
        Parameters:
            arg_descriptors: dict keyed by dest variable name containing sub-dicts as follows:
                'short_flag': '-d', 
                'long_flag': '--debug', 
                'default': <Boolean>, 
                'action': 'store_const', 
                'const': <Boolean>,
                'help': <help string>
                
        Returns:
            argparse namespace object
        """
        logger.debug('Calling _parse_args()')
        log_multiline(logger.debug, arg_descriptors, 'arg_descriptors', '\t')
            
        _arg_parser = argparse.ArgumentParser(description=os.path.basename(sys.argv[0]))
        
        for arg_dest in sorted(arg_descriptors.keys()):
            arg_descriptor = arg_descriptors[arg_dest]
            log_multiline(logger.debug, arg_descriptor, 'arg_descriptor for %s' % arg_dest, '\t')

            _arg_parser.add_argument(arg_descriptor['short_flag'],
                                     arg_descriptor['long_flag'],
                                     dest=arg_dest,
                                     default=arg_descriptor['default'],
                                     action=arg_descriptor['action'],
                                     const=arg_descriptor['const'],
                                     help=arg_descriptor['help']
                                     )
    
        args, _unknown_args = _arg_parser.parse_known_args()
                
        return args.__dict__
    
    def __init__(self, arg_descriptors={}):
        '''Constructor for class CommandLineArgs
        
        Parameters:
            arg_descriptors: dict keyed by dest variable name containing sub-dicts as follows:
                'short_flag': '-d', 
                'long_flag': '--debug', 
                'default': <Boolean>, 
                'action': 'store_const', 
                'const': <Boolean>,
                'help': <help string>
        '''
        log_multiline(logger.debug, arg_descriptors, 'arg_descriptors', '\t')
#        # Replace defaults with supplied dict
#        arg_descriptors = arg_descriptors or CommandLineArgs.DEFAULT_ARG_DESCRIPTORS     
   
        # Merge defaults with supplied dict (overwriting defaults) 
        temp_arg_descriptors = CommandLineArgs.DEFAULT_ARG_DESCRIPTORS.copy()
        temp_arg_descriptors.update(arg_descriptors)
        arg_descriptors = temp_arg_descriptors
        log_multiline(logger.debug, arg_descriptors, 'arg_descriptors', '\t')
        
        self._arguments = self._parse_args(arg_descriptors)
        
        log_multiline(logger.debug, self.__dict__, 'CommandLineArgs.__dict__', '\t')
        
    @property
    def arguments(self):
        return self._arguments.copy()
    
