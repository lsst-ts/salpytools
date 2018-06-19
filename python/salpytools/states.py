'''
Definitions for states of SAL compoments.
Taken from file toolsmod.py in:
https://github.com/lsst/ctrl_iip/blob/master/python/lsst/iip/toolsmod.py
'''

import time
import logging
import inspect
from salpytools.utils import load_SALPYlib

########
# Dictionary showing the state a transition ends in
next_state = {}
next_state["ENTER_CONTROL"] = "STANDBY"
next_state["EXIT_CONTROL"] = "OFFLINE"
next_state["START"] = "DISABLE"
next_state["ENABLE"] = "ENABLE"
next_state["DISABLE"] = "DISABLE"
next_state["STANDBY"] = "STANDBY"
next_state["SET_VALUE"] = "ENABLE"
next_state["ABORT"] = "DISABLE"
next_state["STOP"] = "DISABLE"
# Aliases
next_state["ENTERCONTROL"] = next_state["ENTER_CONTROL"] 
next_state["EXITCONTROL"] = next_state["EXIT_CONTROL"] 


summary_state_enum = {'DISABLE':0,
                      'ENABLE':1, 
                      'FAULT':2, 
                      'OFFLINE':3, 
                      'STANDBY':4}

state_enumeration = {}
state_enumeration["OFFLINE"] = 0
state_enumeration["STANDBY"] = 1
state_enumeration["DISABLE"] = 2
state_enumeration["ENABLE"] =  3
state_enumeration["FAULT"] =   4
state_enumeration["INITIAL"] = 5
state_enumeration["FINAL"] =   6

# This matrix expresses valid transitions and is reproduced in code afterwards.
#
#    \NEXT STATE
#STATE\
#      \ |Offline |Standby |Disabled|Enabled |Fault   |Initial |Final   |
#------------------------------------------------------------------------ 
#Offline | TRUE   | TRUE   |        |        |        |        |  TRUE  |
#------------------------------------------------------------------------
#Standby |  TRUE  | TRUE   |  TRUE  |        |  TRUE  |        |  TRUE  |
#------------------------------------------------------------------------
#Disable |        |  TRUE  |  TRUE  |  TRUE  |  TRUE  |        |        |
#------------------------------------------------------------------------
#Enable  |        |        |  TRUE  |  TRUE  |  TRUE  |        |        |
#------------------------------------------------------------------------
#Fault   |        |        |        |        |  TRUE  |        |        |
#------------------------------------------------------------------------
#Initial |        |  TRUE  |        |        |        | TRUE   |        |
#------------------------------------------------------------------------
#Final   |        |        |        |        |        |        | TRUE   |
#------------------------------------------------------------------------

w, h = 7, 7;
state_matrix = [[False for x in range(w)] for y in range(h)] 
state_matrix[0][6] = True
state_matrix[0][1] = True
state_matrix[1][6] = True
state_matrix[1][0] = True
state_matrix[1][2] = True
state_matrix[1][4] = True
state_matrix[2][1] = True
state_matrix[2][3] = True
state_matrix[2][4] = True
state_matrix[3][2] = True
state_matrix[3][4] = True
state_matrix[5][1] = True

# Set up same state transitions as allowed 
state_matrix[0][0] = True
state_matrix[1][1] = True
state_matrix[2][2] = True
state_matrix[3][3] = True
state_matrix[4][4] = True
state_matrix[5][5] = True
state_matrix[6][6] = True


class BaseState:

    def __init__(self, name, device, events=('SummaryState',), tsleep=0.5):
        self.name = name
        self.device = device
        self.tsleep = tsleep

        self.log = logging.getLogger(self.name)

        self.mgr = {}
        self.myData = {}
        self.logEvent = {}
        self.myData_keys = {}
        self.SALPY_lib = load_SALPYlib(self.device)

        for event in events:
            self.subscribe_logEvent(event)

    def subscribe_logEvent(self, eventname):
        """
        Create a subscription for the {Device}_logevent_{eventnname}
        This step need to be done before we call send_logEvent

        :param eventname:
        :return:
        """
        self.mgr[eventname] = getattr(self.SALPY_lib, 'SAL_{}'.format(self.device))()
        self.mgr[eventname].salEvent("{}_logevent_{}".format(self.device, eventname))
        self.logEvent[eventname] = getattr(self.mgr[eventname], 'logEvent_{}'.format(eventname))
        self.myData[eventname] = getattr(self.SALPY_lib, '{}_logevent_{}C'.format(self.device, eventname))()

        self.myData_keys[eventname] = [a[0] for a in inspect.getmembers(self.myData[eventname]) if
                                       not (a[0].startswith('__') and a[0].endswith('__'))]
        self.log.debug('Initializing: {}_logevent_{}'.format(self.device, eventname))

    def send_logEvent(self, eventname, **kwargs):
        """
        Send logevent for an eventname

        :param eventname:
        :param kwargs:
        :return:
        """
        # Populate myData object for keys across logevent
        # self.myData[eventname].timestamp = kwargs.pop('timestamp',time.time())
        self.myData[eventname].priority = kwargs.pop('priority', 1)
        priority = int(self.myData[eventname].priority)

        # Populate myData with the default cases
        if eventname == 'SummaryState':
            self.myData[eventname].SummaryStateValue = state_enumeration[self.name]
        if eventname == 'RejectedCommand':
            rejected_state = kwargs.get('rejected_state')

            self.myData[eventname].commandValue = state_enumeration[next_state[rejected_state]]  # CHECK THIS OUT
            self.myData[eventname].detailedState = state_enumeration[self.name]

        # Override from kwargs
        for key in kwargs:
            setattr(self.myData[eventname], key, kwargs.get(key))

        self.log.debug('Sending {}'.format(eventname))
        self.logEvent[eventname](self.myData[eventname], priority)
        self.log.info('Sent sucessfully {} Data Object'.format(eventname))
        for key in self.myData_keys[eventname]:
            self.log.info('\t{}:{}'.format(key,getattr(self.myData[eventname],key)))
        time.sleep(self.tsleep)
        return True

    def sleep(self):
        self.log.debug('State sleeping')

    def wake(self):
        self.log.debug('State waking')
        self.send_logEvent('SummaryState')



