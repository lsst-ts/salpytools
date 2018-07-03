
import time
import sys
import threading
import logging
import salpytools.states as csc_states
from salpytools.utils import create_logger, load_SALPYlib
import inspect
from importlib import import_module
import copy
import itertools

"""
A Set of Python classes and tools to subscribe to LSST/SAL DDS topics using the ts_sal generated libraries.
The the Main classes in the module are:

- DDSController:  Subscribe and acknowleges Commands for a Device (threaded)
- DDSSubcriber: Subscribe to Command/Telemetry/Event topics for a Device (threaded)
- DDSSend: Generates/send Telemetry, Events or Commands for a Device (non-threaded)
- DeviceState: Class Used by DDSController to store the state of the Commandable-Component/Device

"""

# Here we store the SAL classes and tools that we use to:
# - Control devices
# - Gather telemetry/events
# - Send Control commands (to sim OCS)
# NOTE: all import of SALPY_{moduleName} are done on the fly using the fuction load_SALPYlib()


SAL__CMD_COMPLETE=303
spinner = itertools.cycle(['-', '/', '|', '\\'])

LOGGER = create_logger(name=__name__)

class Context:
    """The Context orchestrates triggers from the states to the model.

    When creating DDSControllers a Context is passed into it. When the
    DDSController recieves commands it delegates these actions to the Context.
    The context then delegates this to the active State. The salpylib allows
    the states to be inherited from to override behavior of these states, in
    face it is intended to be used this way. In essence this gives the
    implementer full control on what the recieved call on the model.

    Attributes:
        subsystem_tag: A string of the name of the CSC we want. Must exactly
        match the Subsystem Tag defined in XML. Link to current Subsystem Tags
        https://stash.lsstcorp.org/projects/TS/repos/ts_xml/browse/sal_interfaces
        default_state: default state defined within states.py of this library.
        tsleep: A sleeper to prevent race conditions when sending log events.
        states: List of states this context will contain. In general use, these
        should be subclassed states with overriden behavior.
    """
    def __init__(self, subsystem_tag, model, default_state='OFFLINE', tsleep=0.5,
                 states=None):

        self.subsystem_tag = subsystem_tag
        self.model = model
        self.current_state = default_state
        self.tsleep = tsleep
        if states is not None:
            self.states = states
        else:
            # Loading default states
            self.states = dict()
            self.states["OFFLINE"] = csc_states.OfflineState(self.subsystem_tag)
            self.states["STANDBY"] = csc_states.StandbyState(self.subsystem_tag)
            self.states["DISABLED"] = csc_states.DisabledState(self.subsystem_tag)
            self.states["ENABLED"] = csc_states.EnabledState(self.subsystem_tag)
            self.states["FAULT"] = csc_states.FaultState(self.subsystem_tag)
            self.states["INITIAL"] = self.states["STANDBY"]
            self.states["FINAL"] = self.states["ENABLED"]

        # Useful debug logging
        self.log = create_logger(level=logging.NOTSET, name=self.subsystem_tag)
        self.log.debug('{} Init beginning'.format(self.subsystem_tag))
        self.log.debug('Starting with default state: {}'.format(default_state))


    def subscribe_list(self,eventlist):
        # Subscribe to list of logEvents
        for eventname in eventlist:
            self.subscribe_logEvent(eventname)

    def send_logEvent(self,eventname,**kwargs):
        ''' Send logevent for an eventname'''
        # Populate myData object for keys across logevent
        # self.myData[eventname].timestamp = kwargs.pop('timestamp',time.time())
        self.myData[eventname].priority  = kwargs.pop('priority',1)
        priority = int(self.myData[eventname].priority)

        # Populate myData with the default cases
        if eventname == 'SummaryState':
            self.myData[eventname].SummaryStateValue = states.state_enumeration[self.current_state]
        if eventname == 'RejectedCommand':
            rejected_state = kwargs.get('rejected_state')
            next_state = states.next_state[rejected_state]
            self.myData[eventname].commandValue = states.state_enumeration[next_state] # CHECK THIS OUT
            self.myData[eventname].detailedState = states.state_enumeration[self.current_state]

        # Override from kwargs
        for key in kwargs:
            setattr(self.myData[eventname],key,kwargs.get(key))

        self.log.info('Sending {}'.format(eventname))
        self.logEvent[eventname](self.myData[eventname], priority)
        self.log.info('Sent sucessfully {} Data Object'.format(eventname))
        for key in self.myData_keys[eventname]:
            self.log.info('\t{}:{}'.format(key,getattr(self.myData[eventname],key)))
        time.sleep(self.tsleep)
        return True

    def subscribe_logEvent(self, eventname):
        """
        Create a subscription for the {Device}_logevent_{eventnname}
        This step need to be done before we call send_logEvent

        :param eventname:
        :return:
        """
        self.mgr[eventname] = getattr(self.SALPY_lib, 'SAL_{}'.format(self.subsystem_tag))()
        self.mgr[eventname].salEvent("{}_logevent_{}".format(self.subsystem_tag, eventname))
        self.logEvent[eventname] = getattr(self.mgr[eventname],'logEvent_{}'.format(eventname))
        self.myData[eventname] = getattr(self.SALPY_lib,'{}_logevent_{}C'.format(self.subsystem_tag, eventname))()

        self.myData_keys[eventname] = [a[0] for a in inspect.getmembers(self.myData[eventname]) if not(a[0].startswith('__') and a[0].endswith('__'))]
        self.log.info('Initializing: {}_logevent_{}'.format(self.subsystem_tag, eventname))

    def get_current_state(self):
        """
        Function to get the current state

        :return:
        """
        return self.model.state

    def validate_transition(self, new_state):

        return validate_transition(self.current_state, new_state)

    def execute_command(self, command):
        """This method delegates commands recieved by a DDSController to the
        state. 

        The model is passed so that the State object may call methods on
        it. Also the state is stored on the model only as a string
        representation. The actual state object is stored on this context
        object as self.states. 

        Attributes:
            command: A string representation of the command recieved by a
            DDSController object.
        """

        current_state = self.states[self.model.state]

        if command == "ENTERCONTROL":
            current_state.enter_control(self.model)

        elif command == "START":
            current_state.start(self.model)

        elif command == "ENABLE":
            current_state.enable(self.model)

        elif command == "DISABLE":
            current_state.disable(self.model)

        elif command == "STANDBY":
            current_state.go_to_standby(self.model)

        elif command == "EXITCONTROL":
            current_state.exit(self.model)

class DDSController(threading.Thread):
    """Class to subscribe and react to Commands for a Context.

    The DDSController requires a Context be passed into it. This is so that the
    command this DDSController is created to watch can delegate the action to
    the Context. When a DDSController object recieves a trigger over SAL, it
    will delegate this action to the Context. The Context then delegates the
    action to the current state. DDSController is very similar to DDSSubcriber,
    but the difference is that this one can send the acks to the Commands.

    A note on DDSController creation: When the SAL library creates commands
    from your XML, it does so using the following format;

                   [subsystem_tag]_command_[command name]

    For example, the command "enterControl" for the subsystem tag "scheduler"
    would be called "scheduler_command_enterControl" on the EFDB database. A
    DDSController object can be created by either defining the full topic name,
    or by defining a subsystem tag and command name.

    Attributes:
        command: String of the command for this DDSController to watch. Must
        match the exact name of the command defined within the EFDB Topic tag.
        subsystem_tag: A string of the name of the CSC we want. Must exactly
        match the Subsystem Tag defined in XML. Link to current Subsystem Tags
        https://stash.lsstcorp.org/projects/TS/repos/ts_xml/browse/sal_interfaces
        topic: Name of the complete topic we wish to subscribe to. If left empty
        we use the command and subsytem_tag.
    """
    def __init__(self, context, command=None, topic=None, threadID='1', tsleep=0.5):

        # Either a command or topic need to be defined to tell this
        # DDSController what topic to subscribe and react to.
        if command is None and topic is None:
            raise ValueError("Either command or topic must be defined")

        threading.Thread.__init__(self)
        self.subsystem_tag = context.subsystem_tag
        self.command = command
        self.COMMAND = self.command.upper()
        if not topic:
            self.topic = "{}_command_{}".format(self.subsystem_tag, self.command)
        else:
            self.topic  = topic
        self.threadID = threadID
        self.tsleep = tsleep
        self.context = context
        self.daemon = True

        # Create a logger
        self.log = create_logger(level=logging.NOTSET, name=self.subsystem_tag)

        # Store to which state this command is going to move up, using the states.next_state dictionary
        self.next_state = csc_states.next_state[self.COMMAND]

        # Subscribe
        self.subscribe()

    def subscribe(self):

        # This section does the equivalent of:
        # self.mgr = SALPY_tcs.SAL_tcs()
        # The steps are:
        # - 'figure out' the SALPY_xxxx subsystem_tag name
        # - find the library pointer using globals()
        # - create a mananger
        # Here we do the equivalent of:
        # mgr.salProcessor("atHeaderService_command_EnterControl")

        self.newControl = False

        # Get the mgr
        SALPY_lib = import_module('SALPY_{}'.format(self.subsystem_tag)) #globals()['SALPY_{}'.format(self.subsystem_tag)]
        self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.subsystem_tag))()
        self.mgr.salProcessor(self.topic)
        self.myData = getattr(SALPY_lib,self.topic+'C')()
        self.log.info("{} controller ready for topic: {}".format(self.subsystem_tag,self.topic))

        # We use getattr to get the equivalent of for our accept and ack command
        # mgr.acceptCommand_EnterControl()
        # mgr.ackCommand_EnterControl
        self.mgr_acceptCommand = getattr(self.mgr,'acceptCommand_{}'.format(self.command))
        self.mgr_ackCommand = getattr(self.mgr,'ackCommand_{}'.format(self.command))

    def run(self):
        self.run_command()

    def run_command(self):
        while True:
            cmdId = self.mgr_acceptCommand(self.myData)
            if cmdId > 0:
                self.reply_to_transition(cmdId)
                self.newControl = True
            time.sleep(self.tsleep)

    def reply_to_transition(self, cmdid):
        """Delegate the command revcieved to the Context object.

        When creating a DDSController object we pass a Context object upon
        instantiation. This allows a DDSController object to effectively call
        methods on the Context, which the Context then delegates the current
        State. By State design pattern, we are letting the states do all error
        handling and state transition rejections.

        Attributes:
            cmdid: ID handle of the command this DDSController is watching.
        """

        self.mgr_ackCommand(cmdid, SAL__CMD_COMPLETE, 0, "Done : OK");
        self.context.execute_command(self.COMMAND)
          
def validate_transition(current_state, new_state):
    """
    Stand-alone function to validate transition. It returns true/false
    """
    current_index = csc_states.state_enumeration[current_state]
    new_index = csc_states.state_enumeration[new_state]
    transition_is_valid = csc_states.state_matrix[current_index][new_index]
    if transition_is_valid:
        LOGGER.info("Transition from {} --> {} is VALID".format(current_state, new_state))
    else:
        LOGGER.info("Transition from {} --> {} is INVALID".format(current_state, new_state))
    return transition_is_valid


class DDSSubcriber(threading.Thread):

    ''' Class to Subscribe to Telemetry, it could a Command (discouraged), Event or Telemetry'''

    def __init__(self, Device, topic, threadID='1', Stype='Telemetry',tsleep=0.01,timeout=3600,nkeep=100):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.Device = Device
        self.topic  = topic
        self.tsleep = tsleep
        self.Stype  = Stype
        self.timeout = timeout
        self.nkeep   = nkeep
        self.daemon = True
        self.subscribe()

    def subscribe(self):

        # This section does the equivalent of:
        # self.mgr = SALPY_tcs.SAL_tcs()
        # The steps are:
        # - 'figure out' the SALPY_xxxx Device name
        # - find the library pointer using globals()
        # - create a mananger

        self.newTelem = False
        self.newEvent = False

        # Load (if not in globals already) SALPY_{deviceName} into class
        self.SALPY_lib = SALPY_lib = import_module('SALPY_{}'.format(self.Device))
        self.mgr = getattr(self.SALPY_lib, 'SAL_{}'.format(self.Device))()

        if self.Stype=='Telemetry':
            self.myData = getattr(self.SALPY_lib,'{}_{}C'.format(self.Device,self.topic))()
            self.mgr.salTelemetrySub("{}_{}".format(self.Device,self.topic))
            # Generic method to get for example: self.mgr.getNextSample_kernel_FK5Target
            self.getNextSample = getattr(self.mgr,"getNextSample_{}".format(self.topic))
            LOGGER.info("{} subscriber ready for Device:{} topic:{}".format(self.Stype,self.Device,self.topic))
        elif self.Stype=='Event':
            self.myData = getattr(self.SALPY_lib,'{}_logevent_{}C'.format(self.Device,self.topic))()
            self.mgr.salEvent("{}_logevent_{}".format(self.Device,self.topic))
            # Generic method to get for example: self.mgr.getEvent_startIntegration(event)
            self.getEvent = getattr(self.mgr,'getEvent_{}'.format(self.topic))
            LOGGER.info("{} subscriber ready for Device:{} topic:{}".format(self.Stype,self.Device,self.topic))
        elif self.Stype=='Command':
            self.myData = getattr(self.SALPY_lib,'{}_command_{}C'.format(self.Device,self.topic))()
            self.mgr.salProcessor("{}_command_{}".format(self.Device,self.topic))
            # Generic method to get for example: self.mgr.acceptCommand_takeImages(event)
            self.acceptCommand = getattr(self.mgr,'acceptCommand_{}'.format(self.topic))
            LOGGER.info("{} subscriber ready for Device:{} topic:{}".format(self.Stype,self.Device,self.topic))

    def run(self):
        ''' The run method for the threading'''
        self.myDatalist = []
        if self.Stype == 'Telemetry':
            self.newTelem = False
            self.run_Telem()
        elif self.Stype == 'Event':
            self.newEvent = False
            self.run_Event()
        elif self.Stype == 'Command':
            self.newCommand = False
            self.run_Command()
        else:
            raise ValueError("Stype=%s not defined\n" % self.Stype)

    def run_Telem(self):
        while True:
            retval = self.getNextSample(self.myData)
            if retval == 0:
                self.myDatalist.append(self.myData)
                self.myDatalist = self.myDatalist[-self.nkeep:] # Keep only nkeep entries
                self.newTelem = True
            time.sleep(self.tsleep)
        return

    def run_Event(self):
        while True:
            retval = self.getEvent(self.myData)
            if retval == 0:
                self.myDatalist.append(self.myData)
                self.myDatalist = self.myDatalist[-self.nkeep:] # Keep only nkeep entries
                self.newEvent = True
            time.sleep(self.tsleep)
        return
    def run_Command(self):
        while True:
            self.cmdId = self.acceptCommand(self.myData)
            if self.cmdId > 0:
                self.myDatalist.append(self.myData)
                self.myDatalist = self.myDatalist[-self.nkeep:] # Keep only nkeep entries
                self.newCommand = True
            time.sleep(self.tsleep)
        return

    def getCurrent(self):
        if len(self.myDatalist) > 0:
            Current = self.myDatalist[-1]
            self.newTelem = False
            self.newEvent = False
        else:
            # Current = None
            # For now we're passing the empty value of the object, we might want to revise this in the future
            LOGGER.info("WARNING: No value received for: '{}' yet, sending empty object anyway".format(self.topic))
            Current = self.myData
        return Current

    def getCurrentTelemetry(self):
        return self.getCurrent()

    def getCurrentEvent(self):
        return self.getCurrent()

    def getCurrentCommand(self):
        return self.getCurrent()

    def waitEvent(self,tsleep=None,timeout=None):

        """ Loop for waiting for new event """
        if not tsleep:
            tsleep = self.tsleep
        if not timeout:
            timeout = self.timeout

        t0 =  time.time()
        while not self.newEvent:
            sys.stdout.flush()
            sys.stdout.write("Wating for %s event.. [%s]" % (self.topic, spinner.next()))
            sys.stdout.write('\r')
            if time.time() - t0 > timeout:
                LOGGER.info("WARNING: Timeout reading for Event %s" % self.topic)
                self.newEvent = False
                break
            time.sleep(tsleep)
        return self.newEvent

    def resetEvent(self):
        ''' Simple function to set it back'''
        self.newEvent=False


class DDSSend:

    '''
    Class to generate/send Telemetry, Events or Commands.
    In the case of a command, the class instance cannot be
    re-used.
    For Events/Telemetry, the same object can be re-used for a given Device,
    '''

    def __init__(self, Device, sleeptime=1, timeout=5):
        self.sleeptime = sleeptime
        self.timeout = timeout
        self.Device = Device
        self.cmd = ''
        self.myData = {}
        self.issueCommand = {}
        self.waitForCompletion = {}
        LOGGER.info("Loading Device: {}".format(self.Device))
        # Load SALPY_lib into the class
        self.SALPY_lib = import_module('SALPY_{}'.format(self.Device))
        self.manager = getattr(self.SALPY_lib, 'SAL_{}'.format(self.Device))()

        # inspect and get valid commands:
        members = inspect.getmembers(self.SALPY_lib)

        for member in members:
            if 'command' in member[0]:
                cmd_name = member[0].split('command_')[-1][:-1]
                self.issueCommand[cmd_name] = getattr(self.manager, 'issueCommand_{}'.format(cmd_name))
                self.myData[cmd_name] = getattr(self.SALPY_lib,'{}_command_{}C'.format(self.Device, cmd_name))()

    def run(self):
        ''' Function for threading'''
        self.waitForCompletion_Command()

    # def get_mgr(self):
    #     # We get the equivalent of:
    #     #  mgr = SALPY_atHeaderService.SAL_atHeaderService()
    #     mgr = getattr(self.SALPY_lib,'SAL_{}'.format(self.Device))()
    #     return mgr

    def send_Command(self, cmd, **kwargs):
        ''' Send a Command to a Device'''
        timeout = int(kwargs.pop('timeout', self.timeout))
        sleeptime = kwargs.pop('sleeptime', self.sleeptime)
        wait_command = kwargs.pop('wait_command', False)

        # Get the mgr handle
        # mgr = self.get_mgr()
        # mgr.salProcessor("{}_command_{}".format(self.Device,cmd))
        # # Get the myData object
        # myData = getattr(self.SALPY_lib,'{}_command_{}C'.format(self.Device,cmd))()
        LOGGER.info('Updating myData object with kwargs')
        self.update_myData(cmd, **kwargs)
        # Make it visible outside
        self.cmd = cmd

        self.timeout = timeout
        # For a Command we need the functions:
        # 1) issueCommand
        # 2) waitForCompletion -- this can be run separately

        LOGGER.info("Issuing command: {}".format(cmd))
        self.manager.salProcessor("{}_command_{}".format(self.Device, cmd))

        self.cmdId = self.issueCommand[cmd](self.myData[cmd])
        self.retval = self.waitForCompletion[cmd](self.cmdId, self.timeout)

        # self.cmdId_time = time.time()
        # if wait_command:
        #     LOGGER.info("Will wait for Command Completion")
        #     self.waitForCompletion_Command()
        # else:
        #     LOGGER.info("Will NOT wait Command Completion")
        return self.cmdId, self.retval

    def waitForCompletion_Command(self):
        LOGGER.info("Wait {} sec for Completion: {}".format(self.timeout,self.cmd))
        retval = self.waitForCompletion(self.cmdId,self.timeout)
        LOGGER.info("Done: {}".format(self.cmd))
        return retval

    def ackCommand(self,cmd,cmdId):
        """ Just send the ACK for a command, it need the cmdId as input"""
        LOGGER.info("Sending ACK for Id: {} for Command: {}".format(cmdId,cmd))
        mgr = self.get_mgr()
        mgr.salProcessor("{}_command_{}".format(self.Device,cmd))
        ackCommand = getattr(mgr,'ackCommand_{}'.format(cmd))
        ackCommand(cmdId, SAL__CMD_COMPLETE, 0, "Done : OK");

    def acceptCommand(self,cmd):
        mgr = self.get_mgr()
        mgr.salProcessor("{}_command_{}".format(self.Device,cmd))
        acceptCommand = getattr(mgr,'acceptCommand_{}'.format(cmd))
        myData = getattr(self.SALPY_lib,'{}_command_{}C'.format(self.Device,cmd))()
        while True:
            cmdId = acceptCommand(myData)
            if cmdId > 0:
                time.sleep(1)
                break
        cmdId = acceptCommand(myData)
        LOGGER.info("Accpeting cmdId: {} for Command: {}".format(cmdId,cmd))
        return cmdId

    def send_Event(self,event,**kwargs):
        ''' Send an Event from a Device'''

        sleeptime = kwargs.pop('sleep_time',self.sleeptime)
        priority  = kwargs.get('priority',1)

        myData = getattr(self.SALPY_lib,'{}_logevent_{}C'.format(self.Device,event))()
        LOGGER.info('Updating myData object with kwargs')
        myData = self.update_myData(myData,**kwargs)
        # Make it visible outside
        self.myData = myData
        # Get the logEvent object to send myData
        mgr = self.get_mgr()
        name = "{}_logevent_{}".format(self.Device,event)
        mgr.salEvent("{}_logevent_{}".format(self.Device,event))
        logEvent = getattr(mgr,'logEvent_{}'.format(event))
        LOGGER.info("Sending Event: {}".format(event))
        logEvent(myData, priority)
        LOGGER.info("Done: {}".format(event))
        time.sleep(sleeptime)

    def send_Telemetry(self,topic,**kwargs):
        ''' Send an Telemetry from a Device'''

        sleeptime = kwargs.pop('sleep_time',self.sleeptime)
        # Get the myData object
        myData = getattr(self.SALPY_lib,'{}_{}C'.format(self.Device,topic))()
        LOGGER.info('Updating myData object with kwargs')
        self.update_myData(myData,**kwargs)
        # Make it visible outside
        self.myData = myData
        # Get the Telemetry object to send myData
        mgr = self.get_mgr()
        mgr.salTelemetryPub("{}_{}".format(self.Device,topic))
        putSample = getattr(mgr,'putSample_{}'.format(topic))
        LOGGER.info("Sending Telemetry: {}".format(topic))
        putSample(myData)
        LOGGER.info("Done: {}".format(topic))
        time.sleep(sleeptime)

    def update_myData(self, cmd, **kwargs):
        """ Updating myData with kwargs """
        myData_keys = [a[0] for a in inspect.getmembers(self.myData[cmd]) if not(a[0].startswith('__')
                                                                                 and a[0].endswith('__'))]
        for key in kwargs:
            if key in myData_keys:
                LOGGER.info('{} = {}'.format(key, kwargs.get(key)))
                setattr(self.myData[cmd], key, kwargs.get(key))
            else:
                LOGGER.info('key {} not in myData'.format(key))

    def get_myData(self):
        """ Make a dictionary representation of the myData C objects"""
        myData_dic = {}
        myData_keys = [a[0] for a in inspect.getmembers(self.myData) if not(a[0].startswith('__') and a[0].endswith('__'))]
        for key in myData_keys:
            myData_dic[key] =  getattr(self.myData,key)
        return myData_dic

def command_sequencer(commands,Device='atHeaderService',wait_time=1, sleep_time=3):

    """
    Stand-alone function to send a sequence of OCS Commands
    """

    # We get the equivalent of:
    #  mgr = SALPY_atHeaderService.SAL_atHeaderService()
    # Load (if not in globals already) SALPY_{deviceName}
    SALPY_lib = load_SALPYlib(Device)

    mgr = getattr(SALPY_lib,'SAL_{}'.format(Device))()
    myData = {}
    issueCommand = {}
    waitForCompletion = {}
    for cmd in commands:
        myData[cmd] = getattr(SALPY_lib,'{}_command_{}C'.format(Device,cmd))()
        issueCommand[cmd] = getattr(mgr,'issueCommand_{}'.format(cmd))
        waitForCompletion[cmd] = getattr(mgr,'waitForCompletion_{}'.format(cmd))
        # If Start we send some non-sense value
        if cmd == 'Start':
            myData[cmd].configure = 'blah.json'

    for cmd in commands:
        LOGGER.info("Issuing command: {}".format(cmd))
        LOGGER.info("Wait for Completion: {}".format(cmd))
        cmdId = issueCommand[cmd](myData[cmd])
        waitForCompletion[cmd](cmdId,wait_time)
        LOGGER.info("Done: {}".format(cmd))
        time.sleep(sleep_time)

    return
