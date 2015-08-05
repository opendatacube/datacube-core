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
import ConfigParser
import collections

from _gdfutils import log_multiline

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Logging level for this module

class ConfigFile(object):
    def _parse_config_file(self):
        '''
        Function to return a nested dict of config file entries
        Returns: 
            dict {<section_name>: {<key>: <value>,... },... }
        '''
        logger.debug('Opening config file %s', self._path)
        
        config_parser = ConfigParser.SafeConfigParser(allow_no_value=True)
        config_parser.read(self._path)
        
        config_dict = collections.OrderedDict() # Need to preserve order of sections
        for section_name in config_parser.sections():
            section_dict = {}
            config_dict[section_name.lower()] = section_dict
            
            for attribute_name in config_parser.options(section_name):
                attribute_value = config_parser.get(section_name, attribute_name)
                section_dict[attribute_name.lower()] = attribute_value
                
        log_multiline(logger.debug, config_dict, 'config_dict', '\t')
        return config_dict
    
    def __init__(self, path):
        '''Constructor for class ConfigFile
        
        Parameters:
            path: Path to valid config file (required)
        '''
        log_multiline(logger.debug, path, 'path', '\t')
        
        self._path = os.path.abspath(path)
        assert os.path.exists(self._path), "%s does not exist" % self._path
        
        self._configuration = self._parse_config_file() 
        
        log_multiline(logger.debug, self.__dict__, 'ConfigFile.__dict__', '\t')
        
    @property
    def path(self):
        return self._path
    
    @property
    def configuration(self):
        return self._configuration.copy()
    
