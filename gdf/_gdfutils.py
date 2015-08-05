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
Miscellaneous utilities for GDF

Created on Jun 11, 2015

@author: Alex Ip
'''
import os
import calendar
import time
import math
from datetime import datetime, date
from socket import errno
import logging
from pprint import pformat

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Logging level for this module

# Ignore failed import of graphical modules
try:
    import matplotlib.pyplot as plt
except:
    logger.warning('WARNING: Unable to import matplotlib.pyplot. Any graphical function calls will fail.')

#TODO: Do something about duplicate definition (here and in gdf)
EPOCH_DATE_ORDINAL = date(1970, 1, 1).toordinal()

def dt2secs(datetime_param):
    '''
    Helper function to convert datetime into seconds since epoch. Naive datetime is treated as UTC
    '''
    return calendar.timegm(datetime_param.timetuple())

def secs2dt(seconds_param):
    '''
    Helper function to convert seconds since epoch into datetime. Naive datetime is treated as UTC
    '''
    return datetime.fromtimestamp(seconds_param)

def dt2days(datetime_param):
    '''
    Helper function to convert datetime into days since epoch. Naive datetime is treated as UTC
    '''
    return datetime_param.toordinal() - EPOCH_DATE_ORDINAL

def days2dt(days_param):
    '''
    Helper function to convert days since epoch into datetime. Naive datetime is treated as UTC
    '''
    return datetime.fromordinal(days_param + EPOCH_DATE_ORDINAL)

def make_dir(dirname):
    '''
    Function to create a specified directory if it doesn't exist
    '''
    try:
        os.makedirs(dirname)
    except OSError, exception:
        if exception.errno != errno.EEXIST or not os.path.isdir(dirname):
            raise exception

def directory_writable(dir_path):
    '''
    Function to return true if dir_path can be written to
    '''
    try:
        make_dir(dir_path)
        test_filename = os.path.join(dir_path, 'test')
        test_file = open(test_filename, 'w')
        test_file.close()
        os.remove(test_filename)
        return True
    except:
        return False
                
            
def plotImages(arrays):
    img = arrays
    num_t = img.shape[0]
    num_rowcol = math.ceil(math.sqrt(num_t))
    fig = plt.figure()
    fig.clf()
    plot_count = 1
    for i in range(img.shape[0]):
        data = img[i]
        ax = fig.add_subplot(num_rowcol, num_rowcol, plot_count)
        plt.setp(ax, xticks=[], yticks=[])
        cax = ax.imshow(data, interpolation='nearest', aspect = 'equal')
        #fig.colorbar(cax)
        plot_count += 1
    fig.tight_layout()
    plt.show()

def log_multiline(log_function, log_text, title=None, prefix=''):
    """Function to log multi-line text
    """
    logger.debug('log_multiline(%s, %s, %s, %s) called', log_function, repr(log_text), repr(title), repr(prefix))

    if isinstance(log_text, str):
        logger.debug('log_text is type str')
        log_list = log_text.splitlines()
    elif isinstance(log_text, list) and isinstance(log_text[0], str):
        logger.debug('log_text is type list with first element of type text')
        log_list = log_text
    else:
        logger.debug('log_text is type ' + type(log_text).__name__)
        log_list = pformat(log_text).splitlines()

    log_function(prefix + '=' * 80)
    if title:
        log_function(prefix + title)
        log_function(prefix + '-' * 80)

    for line in log_list:
        log_function(prefix + line)

    log_function(prefix + '=' * 80)
