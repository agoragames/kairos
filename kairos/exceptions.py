'''
Copyright (c) 2012-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''

class KairosException(Exception):
  '''Base class for all kairos exceptions'''

class UnknownInterval(KairosException):
  '''The requested interval is not configured.'''
