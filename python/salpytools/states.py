'''
Definitions for states of SAL compoments.
Taken from file toolsmod.py in:
https://github.com/lsst/ctrl_iip/blob/master/python/lsst/iip/toolsmod.py
'''

import time
import logging
import inspect
from salpytools.utils import load_SALPYlib
from salpytools.state_transition_exception import StateTransitionException

summary_state_enum = {'DISABLE':0,
                      'ENABLE':1,
                      'FAULT':2,
                      'OFFLINE':3,
                      'STANDBY':4}


class DefaultState:

    def __init__(self, name, subsystem_tag, events=('summaryState',), tsleep=0.5):
        self.name = name
        self.subsystem_tag = subsystem_tag
        self.tsleep = tsleep

        self.log = logging.getLogger(self.name)

        self.mgr = {}
        self.myData = {}
        self.logEvent = {}
        self.myData_keys = {}
        self.SALPY_lib = load_SALPYlib(self.subsystem_tag)

        for event in events:
            self.subscribe_logEvent(event)

    def subscribe_logEvent(self, eventname):
        """
        Create a subscription for the {subsystem_tag}_logevent_{eventnname}
        This step need to be done before we call send_logEvent

        :param eventname:
        :return:
        """
        self.mgr[eventname] = getattr(self.SALPY_lib, 'SAL_{}'.format(self.subsystem_tag))()
        self.mgr[eventname].salEvent("{}_logevent_{}".format(self.subsystem_tag, eventname))
        self.logEvent[eventname] = getattr(self.mgr[eventname], 'logEvent_{}'.format(eventname))
        self.myData[eventname] = getattr(self.SALPY_lib, '{}_logevent_{}C'.format(self.subsystem_tag, eventname))()

        self.myData_keys[eventname] = [a[0] for a in inspect.getmembers(self.myData[eventname]) if
                                       not (a[0].startswith('__') and a[0].endswith('__'))]
        self.log.debug('Initializing: {}_logevent_{}'.format(self.subsystem_tag, eventname))

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
        self.send_logEvent('summaryState')

    #<----- Default State methods corresponding to UML design under here ------>

    def disable(self, model):
        raise StateTransitionException()

    def enable(self, model):
        raise StateTransitionException()

    def exit_control(self, model):
        raise StateTransitionException()

    def standby(self, model):
        raise StateTransitionException()

    def start(self, model):
        raise StateTransitionException()

    def enter_control(self, model):
        raise StateTransitionException()

    def exit(self, model):
        self.log.debug("Default: exit() not implemented")

    def do(self, model):
        self.log.debug("Default: do() not implemented")



class OfflineState(DefaultState):

    def __init__(self, subsystem_tag, events=('summaryState',), tsleep=0.5):
        super(OfflineState, self).__init__('OFFLINE', subsystem_tag, events, tsleep)

    def enter_control(self, model):
        model.change_state("STANDBY")
        self.send_logEvent("summaryState", summaryState=5)
        return 0, 'DONE'

    def exit(self, model):
        self.log.debug("Offline: exit() not implemented")

    def do(self, model):
        self.log.debug("Offline: do() not implemented")


class StandbyState(DefaultState):

    def __init__(self, subsystem_tag, events=('summaryState',), tsleep=0.5):
        super(StandbyState, self).__init__('STANDBY', subsystem_tag, events, tsleep)

    def exit_control(self, model):
        model.change_state("OFFLINE")
        self.send_logEvent("summaryState", summaryState=4)
        return 0, 'DONE'

    def start(self, model):
        model.change_state("DISABLED")
        self.send_logEvent("summaryState", summaryState=1)
        return 0, 'DONE'

    def exit(self, model):
        self.log.debug("Standby: exit() not implemented")

    def do(self, model):
        self.log.debug("Standby: do() not implemented")

    def on_heartbeat(self, model):
        pass

class DisabledState(DefaultState):

    def __init__(self, subsystem_tag, events=('summaryState',), tsleep=0.5):
        super(DisabledState, self).__init__('DISABLED', subsystem_tag, events, tsleep)

    def enable(self, model):
        model.change_state("ENABLED")
        self.send_logEvent("summaryState", summaryState=2)
        return 0, 'DONE'

    def standby(self, model):
        model.change_state("STANDBY")
        self.send_logEvent("summaryState", summaryState=5)
        return 0, 'DONE'

    def exit(self, model):
        self.log.debug("Disabled: exit() not implemented")

    def do(self, model):
        self.log.debug("Disabled: do() not implemented")

    def on_heartbeat(self, model):
        pass

    def on_incoming_messaging_error(self, model):
        pass

    def on_interrupt_end_loop(self, model):
        pass

    def on_interrupt_process_triggers(self, model):
        pass

class EnabledState(DefaultState):

    def __init__(self, subsystem_tag, events=('summaryState',), tsleep=0.5):
        super(EnabledState, self).__init__('ENABLED', subsystem_tag, events, tsleep)

    def disable(self, model):
        model.change_state("DISABLED")
        self.send_logEvent("summaryState", summaryState=1)
        return 0, 'DONE'

    def exit(self, model):
        self.log.debug("Enabled: exit() not implemented")

    def do(self, model):
        self.log.debug("Enabled: do() not implemented")

    def on_hearbeat(self, model):
        pass

    def on_incoming_messaging_error(self, model):
        pass

    def on_interrupt_end_loop(self, model):
        pass

    def on_interrupt_process_triggers(self, model):
        pass

class FaultState(DefaultState):

    def __init__(self, subsystem_tag, events=('summaryState',), tsleep=0.5):
        super(FaultState, self).__init__('FAULT', subsystem_tag, events, tsleep)

    def go_to_standby(self, model):
        self.model.change_state("STANDBY")
        self.send_logEvent("summaryState", summaryState=4)
        return 0, 'DONE'

    def on_heartbeat(self, model):
        pass

    def on_incoming_messaging_error(self, model):
        pass

    def on_interrupt_end_loop(self, model):
        pass