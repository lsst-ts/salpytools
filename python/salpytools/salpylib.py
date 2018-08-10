
import time
import sys
import threading
import salpytools.states as csc_states
from salpytools.utils import create_logger, load_SALPYlib
import inspect
from importlib import import_module
import itertools
import logging

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


SAL__CMD_ABORTED = -303
SAL__CMD_ACK = 300
SAL__CMD_COMPLETE = 303
SAL__CMD_FAILED = -302
SAL__CMD_INPROGRESS = 301
SAL__CMD_NOACK = -301
SAL__CMD_NOPERM = -300
SAL__CMD_STALLED = 302
SAL__CMD_TIMEOUT = -304

spinner = itertools.cycle(['-', '/', '|', '\\'])

LOGGER = create_logger(name=__name__)


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
    def __init__(self, context, command=None, topic=None, device_id=None, threadID='1', tsleep=0.5):

        # Either a command or topic need to be defined to tell this
        # DDSController what topic to subscribe and react to.
        if command is None and topic is None:
            raise ValueError("Either command or topic must be defined")

        threading.Thread.__init__(self)
        self.subsystem_tag = context.subsystem_tag
        self.device_id = device_id
        self.command = command
        self.COMMAND = self.command.upper()

        # Create a logger
        self.log = logging.getLogger(self.subsystem_tag)

        self.log.debug('Starting DDSController for {}:{}'.format(self.subsystem_tag,
                                                                 self.COMMAND))

        if not topic:
            self.topic = "{}_command_{}".format(self.subsystem_tag, self.command)
        else:
            self.topic  = topic
        self.threadID = threadID
        self.tsleep = tsleep
        self.context = context
        self.daemon = True

        self.newControl = False

        # Subscribe
        self.mgr = None  # SAL Manager
        self.myData = None  # SAL topic
        self.mgr_acceptCommand = None  # Accept command
        self.mgr_ackCommand = None  # Ack command

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
        # Get the mgr
        SALPY_lib = import_module('SALPY_{}'.format(self.subsystem_tag))

        if self.device_id is None:
            self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.subsystem_tag))()
        else:
            try:
                self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.subsystem_tag))(self.device_id)
            except TypeError:
                self.log.error('Could not initialize component {} '
                               'with device id {}. Trying with no id.'.format(self.subsystem_tag, self.device_id))
                self.device_id = None
                self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.subsystem_tag))()

        # self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.subsystem_tag))()
        self.mgr.salProcessor(self.topic)
        self.myData = getattr(SALPY_lib, self.topic+'C')()
        self.log.info("{} controller ready for topic: {}".format(self.subsystem_tag,self.topic))

        # We use getattr to get the equivalent of for our accept and ack command
        # mgr.acceptCommand_EnterControl()
        # mgr.ackCommand_EnterControl
        self.mgr_acceptCommand = getattr(self.mgr, 'acceptCommand_{}'.format(self.command))
        self.mgr_ackCommand = getattr(self.mgr, 'ackCommand_{}'.format(self.command))

    def run(self):
        self.log.debug('Running...')
        self.run_command()

    def run_command(self):
        while True:
            cmdId = self.mgr_acceptCommand(self.myData)
            if cmdId > 0:
                self.mgr_ackCommand(cmdId, SAL__CMD_ACK, 0, "Command received : OK")
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

        self.mgr_ackCommand(cmdid, SAL__CMD_INPROGRESS, 0, "Starting: OK")
        try:
            self.log.debug('Starting command execution ...')
            err, message = self.context.execute_command(self.COMMAND, self.myData)
            self.log.debug('Command execution complete...')
        except Exception as exception:
            self.log.error('Exception while executing {}.'.format(self.COMMAND))
            self.mgr_ackCommand(cmdid, SAL__CMD_FAILED, 1,
                                "An {} exception occurred when running {}.".format(exception.__class__.__name__,
                                                                                   self.COMMAND))
        else:
            self.log.debug('Sending {} ack with {} {}'.format(SAL__CMD_COMPLETE,
                                                              err,
                                                              message))
            self.mgr_ackCommand(cmdid, SAL__CMD_COMPLETE, err, message)


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


class DDSSubscriber(threading.Thread):

    ''' Class to Subscribe to Telemetry, it could a Command (discouraged), Event or Telemetry'''

    def __init__(self, Device, topic, device_id=None, threadID='1', Stype='Telemetry',
                 tsleep=0.01, timeout=3600, nkeep=100):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.Device = Device
        self.topic = topic
        self.device_id = device_id

        self.log = create_logger(name=self.Device)

        self.tsleep = tsleep
        self.Stype  = Stype
        self.timeout = timeout
        self.nkeep   = nkeep
        self.daemon = True

        # Subscribe
        self.newTelem = False
        self.newEvent = False

        self.getNextSample = None  # Method to get telemetry
        self.getEvent = None  # Method to get Event
        self.myData = None  # Method to get Commands
        self.acceptCommand = None  # Method to accept command

        self.mgr = None  # SAL Manager

        self.subscribe()

    def subscribe(self):

        # This section does the equivalent of:
        # self.mgr = SALPY_tcs.SAL_tcs()
        # The steps are:
        # - 'figure out' the SALPY_xxxx Device name
        # - find the library pointer using globals()
        # - create a mananger

        SALPY_lib = import_module('SALPY_{}'.format(self.Device))

        if self.device_id is None:
            self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.Device))()
        else:
            try:
                self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.Device))(self.device_id)
            except TypeError:
                self.log.error('Could not initialize component {} '
                               'with device id {}. Trying with no id.'.format(self.Device, self.device_id))
                self.device_id = None
                self.mgr = getattr(SALPY_lib, 'SAL_{}'.format(self.Device))()

        if self.Stype == 'Telemetry':
            self.myData = getattr(SALPY_lib, '{}_{}C'.format(self.Device, self.topic))()
            self.mgr.salTelemetrySub("{}_{}".format(self.Device, self.topic))
            # Generic method to get for example: self.mgr.getNextSample_kernel_FK5Target
            self.getNextSample = getattr(self.mgr, "getNextSample_{}".format(self.topic))
            self.log.debug("{} subscriber ready for Device:{} topic:{}".format(self.Stype, self.Device, self.topic))
        elif self.Stype == 'Event':
            self.myData = getattr(SALPY_lib, '{}_logevent_{}C'.format(self.Device, self.topic))()
            self.mgr.salEvent("{}_logevent_{}".format(self.Device, self.topic))
            # Generic method to get for example: self.mgr.getEvent_startIntegration(event)
            self.getEvent = getattr(self.mgr, 'getEvent_{}'.format(self.topic))
            self.log.debug("{} subscriber ready for Device:{} topic:{}".format(self.Stype, self.Device, self.topic))
        elif self.Stype == 'Command':
            self.log.warning('This method is not intended to be used to listen to commands. Unless you know what you'
                             'are doing, you are probably looking for DDSController instead.')
            self.myData = getattr(SALPY_lib, '{}_command_{}C'.format(self.Device, self.topic))()
            self.mgr.salProcessor("{}_command_{}".format(self.Device, self.topic))
            # Generic method to get for example: self.mgr.acceptCommand_takeImages(event)
            self.acceptCommand = getattr(self.mgr, 'acceptCommand_{}'.format(self.topic))
            self.log.debug("{} subscriber ready for Device:{} topic:{}".format(self.Stype, self.Device, self.topic))

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
            self.log.warning("No value received for: '{}' yet, sending empty object anyway".format(self.topic))
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
                self.log.warning("Timeout reading for Event %s" % self.topic)
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

    def __init__(self, Device, device_id=None, sleeptime=1, timeout=5):
        self.sleeptime = sleeptime
        self.timeout = timeout
        self.Device = Device
        self.device_id = device_id
        self.cmd = ''
        self.log = create_logger(name=self.Device)
        self.log.debug("Loading Device: {}".format(self.Device))

        # Load SALPY_lib into the class
        self.SALPY_lib = import_module('SALPY_{}'.format(self.Device))
        if device_id is None:
            self.manager = getattr(self.SALPY_lib, 'SAL_{}'.format(self.Device))()
        else:
            try:
                self.manager = getattr(self.SALPY_lib, 'SAL_{}'.format(self.Device))(device_id)
            except TypeError:
                self.log.error('Could not initialize component {} '
                               'with device id {}. Trying with no id.'.format(self.Device, device_id))
                self.device_id = None
                self.manager = getattr(self.SALPY_lib, 'SAL_{}'.format(self.Device))()

    def send_Command(self, cmd, **kwargs):
        """
         Send a Command to a Device

        :param cmd:
        :param kwargs:
        :return:
        """

        timeout = int(kwargs.pop('timeout', self.timeout))
        wait_command = kwargs.pop('wait_command', False)

        self.log.debug('Updating myData object with kwargs')
        data = self.get_cmd_data(cmd, **kwargs)
        # Make it visible outside

        self.timeout = timeout
        # For a Command we need the functions:
        # 1) issueCommand
        # 2) waitForCompletion -- this can be run separately

        self.log.debug("Issuing command: {}".format(cmd))
        self.manager.salProcessor("{}_command_{}".format(self.Device, cmd))
        cmdid = getattr(self.manager, 'issueCommand_{}'.format(cmd))(data)

        if wait_command:
            retval = self.waitForCompletion(cmd, cmdid, timeout)
        else:
            retval = None

        return cmdid, retval

    def waitForCompletion(self, cmd, cmdid, timeout=None):

        tout = timeout if timeout is not None else self.timeout
        self.log.debug("Wait {} sec for Completion: {}[{}]".format(tout, cmd, cmdid))
        retval = getattr(self.manager, 'waitForCompletion_{}'.format(cmd))(cmdid, tout)
        self.log.debug("Done: {}".format(cmd))
        return retval

    def ackCommand(self, cmd, cmdId):
        """ Just send the ACK for a command, it need the cmdId as input"""
        self.log.debug("Sending ACK for Id: {} for Command: {}".format(cmdId,cmd))
        self.manager.salProcessor("{}_command_{}".format(self.Device,cmd))
        ackCommand = getattr(self.manager, 'ackCommand_{}'.format(cmd))
        ackCommand(cmdId, SAL__CMD_COMPLETE, 0, "Done : OK");

    def acceptCommand(self, cmd):
        mgr = self.manager
        mgr.salProcessor("{}_command_{}".format(self.Device,cmd))
        acceptCommand = getattr(mgr, 'acceptCommand_{}'.format(cmd))
        myData = getattr(self.SALPY_lib, '{}_command_{}C'.format(self.Device,cmd))()
        while True:
            cmdId = acceptCommand(myData)
            if cmdId > 0:
                time.sleep(1)
                break
        cmdId = acceptCommand(myData)
        self.log.debug("Accepting cmdId: {} for Command: {}".format(cmdId,cmd))
        return cmdId

    def send_Event(self, event, **kwargs):
        """
        Publish an Event.

        :param event:
        :param kwargs:
        :return:
        """

        priority = kwargs.get('priority', 1)

        data = self.get_event_data(event, **kwargs)

        # Get the logEvent object to send myData

        self.manager.salEvent("{}_logevent_{}".format(self.Device, event))

        self.log.debug("Sending Event: {}".format(event))
        getattr(self.manager, 'logEvent_{}'.format(event))(data, priority)

        self.log.debug("Done: {}".format(event))

    def send_Telemetry(self, telemetry, **kwargs):
        """
        Publish Telemetry.

        :param telemetry:
        :param kwargs:
        :return:
        """

        # Get the myData object
        data = self.get_telemetry_data(telemetry, **kwargs)

        # Make it visible outside

        self.manager.salTelemetryPub("{}_{}".format(self.Device, telemetry))
        self.log.debug("Sending Telemetry: {}".format(telemetry))
        getattr(self.manager, 'putSample_{}'.format(telemetry))(data)

    def get_cmd_data(self, cmd, **kwargs):
        return self.get_data('{}_command_{}C'.format(self.Device, cmd), **kwargs)

    def get_event_data(self, event, **kwargs):
        return self.get_data('{}_logevent_{}C'.format(self.Device, event), **kwargs)

    def get_telemetry_data(self, telemetry, **kwargs):
        return self.get_data('{}_{}C'.format(self.Device, telemetry), **kwargs)

    def get_data(self, name, **kwargs):
        """ Updating myData with kwargs """
        data = getattr(self.SALPY_lib, name)()

        for key in kwargs:
            try:
                setattr(data, key, kwargs.get(key))
            except AttributeError:
                self.log.warning('No {} in {}() [skipping]'.format(key, name))
            else:
                self.log.debug('{} = {}'.format(key, kwargs.get(key)))

        return data


class DDSSubscriberContainer:
    '''
    This utility class will subscribe to all or a specific event from a specified controller and provide high-level
    object-oriented access to the underlying data.
    '''

    def __init__(self, device, stype='Event', topic=None, tsleep=0.1):

        self.device = device
        self.type = stype

        self.tsleep = tsleep

        self.subscribers = {}

        self.log = create_logger(name=self.device)

        self.log.debug("Loading Device: {}".format(self.device))
        # Load SALPY_lib into the class
        self.SALPY_lib = import_module('SALPY_{}'.format(self.device))
        self.manager = getattr(self.SALPY_lib, 'SAL_{}'.format(self.device))()

        if topic is not None:
            self.log.debug("Loading topic: {}".format(topic))
            self.topic = [topic]
        else:
            # Inspect device type to get all topics
            self.topic = []
            self.log.debug("Loading all topics from {}".format(self.device))

            # inspect and get valid commands:
            members = inspect.getmembers(self.SALPY_lib)

            def checker(_name, _type, _device):
                if _type == 'Event' and '_logevent_' in _name:
                    return True
                elif ((_type == 'Telemetry') and (_device + '_' in _name) and
                      ('_logevent_' not in _name) and ('command' not in _name)):
                    return True
                else:
                    return False

            break_string = '_logevent_' if self.topic == 'Event' else '_'

            for member in members:
                if checker(member[0], self.type, self.device):
                    name = member[0].split(break_string)[-1][:-1]
                    self.log.debug('Adding {}...'.format(name))
                    self.topic.append(name)
                    try:
                        self.subscribers[name] = DDSSubscriber(Device=self.device,
                                                               topic=name,
                                                               Stype=self.type,
                                                               threadID='{}_{}_{}'.format(self.device, self.type, name),
                                                               tsleep=self.tsleep)
                        self.subscribers[name].start()
                    except AttributeError:
                        self.log.debug('Could not add {}... Skipping...'.format(name))
                    else:
                        setattr(self, name, self.subscribers[name].myData)

    def __getattr__(self, item):
        if item in self.topic:
            return self.subscribers[item].getCurrent()
        else:
            raise AttributeError('No attribute ' + item)

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
