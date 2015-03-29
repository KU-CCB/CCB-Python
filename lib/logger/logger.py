"""
Logger

Logger writes messages to log files that match the script name of the caller. 
For example, if foo.py calls logger.log('Bar'), the "> [the-date-and-time] Bar" 
gets written to "/log/root/foo.log" where '/log/root/' is replaced by the path 
set in config.cfg as 'logDir'
"""

import inspect
import datetime
import os
import sys
import ConfigParser

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
logDir = cfg.get('default', 'logDir')

# Ways to display the date in the logfile
_DATE_FORMAT_WORDS   = "%c"
_DATE_FORMAT_NUMERIC = "%Y-%m-%d:%X"

# Indicators of the type of message logged
_INDICATOR_DEFAULT = '>'
_INDICATOR_WARNING = '!'
_INDICATOR_ERROR   = 'x'

def _writeMessage(logpath, msg, indicator):
  d = datetime.datetime.today()
  with open(logpath, 'a') as logfile:
    logfile.write("%c (%s) %s\n" % 
      (indicator, d.strftime(_DATE_FORMAT_NUMERIC), msg))

def log(msg):
  curframe = inspect.currentframe()
  calframe = inspect.getouterframes(curframe, 2)
  caller = os.path.splitext(os.path.split(calframe[1][1])[1])[0]
  logpath = "%s/%s.log" % (logDir, caller)
  _writeMessage(logpath, msg,  _INDICATOR_DEFAULT)

def warn(msg):
  curframe = inspect.currentframe()
  calframe = inspect.getouterframes(curframe, 2)
  caller = os.path.splitext(os.path.split(calframe[1][1])[1])[0]
  logpath = "%s/%s.log" % (logDir, caller)
  _writeMessage(logpath, msg,  _INDICATOR_WARNING)

def error(msg):
  curframe = inspect.currentframe()
  calframe = inspect.getouterframes(curframe, 2)
  caller = os.path.splitext(os.path.split(calframe[1][1])[1])[0]
  logpath = "%s/%s.log" % (logDir, caller)
  _writeMessage(logpath, msg,  _INDICATOR_ERROR)

