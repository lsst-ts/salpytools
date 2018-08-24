
import time
import sys
import threading
import salpytools.states as csc_states
from salpytools.utils import create_logger, load_SALPYlib
import inspect
from importlib import import_module
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
        self.log = create_logger(name=self.subsystem_tag)

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

        self.mgr_ackCommand(cmdid, SAL__CMD_INPROGRESS, 0, "Starting: OK")
        try:
            err, message = self.context.execute_command(self.COMMAND, self.myData)
        except Exception as exception:
            self.mgr_ackCommand(cmdid, SAL__CMD_FAILED, 1,
                                "An {} exception occurred when running {}.".format(exception.__class__.__name__,
                                                                                   self.COMMAND))
        else:
            self.mgr_ackCommand(cmdid, SAL__CMD_COMPLETE, err, message)

class DDSSubscriberThread(threading.Thread):
    """Subscribes either to Telemetry or Events and saves the received data to
    a handed dictionary. In this implementation a new thread is used for every
    topic.
    Attributes:
        topic: A string of the name of the topic we are subscribing to.
        handle: An dictionary to save the contents of the recieved data.
        rate: Rate at which we attempt to obtain new data.
        timeout: How long to wait for new data before timing out.
    """
    def __init__(self, topic=None, handle={}, rate=1, timeout=10):
        threading.Thread.__init__(self)

        self.topic = topic
        if self.topic is None:
            raise ValueError("A 'topic' argument must be passed")

        self.handle = handle
        if type(self.handle) is not dict:
            raise ValueError("'handle' argument must be of type dictionary")

        self.rate = rate
        self.timeout = timeout

        # Attributes we need to interact with SAL
        self.subsystem_tag = self.parse_for_subsystem_tag(topic)
        self.short_topic = self.parse_for_short_topic_name(topic)
        self.salpy_lib = None
        self.mgr = None
        self.data = None

        # We will turn only a single flag to true on mgr_subscibe_to_topic()
        self.is_command = False
        self.is_event = False
        self.is_telemetry = False

    def parse_for_subsystem_tag(self, topic):
        """Here we parse our the subsytem tag from the topic. Must exactly
        match the Subsystem Tag defined in XML. Link to current Subsystem Tags
        https://stash.lsstcorp.org/projects/TS/repos/ts_xml/browse/sal_interfaces

        There are a few rules that the topic must follow and will help with
        raising accurate errors. If the topic is;

        Telemetry: It will only have (1) underscore, after the subsytem tag.
        Followed by any arbitrary set of chars for the name of the telemetry.

                               [subsystem_tag]_[a-zA-Z]
                                        ex;
                                   scheduler_seeing

        Event: It will have (2) underscores. One after the subsytem tag,
        followed by "logevent", then the second underscore. Ending with an
        arbitary set of chars for the name of the event.

                         [subsystem_tag]_logevent_[a-zA-Z]
                                        ex;
                              scheduler_logevent_target

        Command: Similiar to event, only difference being is that it will have
        "command" rather than "logevent"

                         [subsystem_tag]_command_[a-zA-Z]
                                        ex;
                              scheduler_command_target
        """

        parsed_topic = topic.split('_')

        if len(parsed_topic[0]) == 0:     # Empty leading string
            raise ValueError("Please check Topic format")

        if len(parsed_topic[-1]) == 0:    # Empty ending string
            raise ValueError("Please check Topic format")

        if len(parsed_topic) < 1:         # No underscores in string
            raise ValueError("Please check Topic format")

        if len(parsed_topic) > 3:         # Too many underscores in string
            raise ValueError("Please check Topic format")

        if len(parsed_topic) == 3:        # Middle word not logevent or command

            if parsed_topic[1] == "command" or parsed_topic[1] == "logevent":
                return parsed_topic[0]
            else:
                raise ValueError("Please check Topic format")

        if len(parsed_topic) == 2:
            return parsed_topic[0]

        raise ValueError("Please check topic format")

    def parse_for_short_topic_name(self, topic):

        parsed_topic = topic.split("_")

        return parsed_topic[-1]

    def configure(self):               # Example Equivilent of...
        self.set_salpy_lib()           # import SALPY_scheduler
        self.set_mgr()                 # self.mgr = SAL_scheduler()
        self.mgr_subscribe_to_topic()  # self.mgr.salEvent("scheduler_[topic]")
        self.set_data()                # self.data = scheduler_logevent_[topic]C

    def set_salpy_lib(self):
        self.salpy_lib = import_module('SALPY_{}'.format(self.subsystem_tag))

    def set_mgr(self):
        self.mgr = getattr(self.salpy_lib, 'SAL_{}'.format(self.subsystem_tag))()

    def mgr_subscribe_to_topic(self):
        """The topic can be Telemetry, a Command, or an Event. We know which it
        is because the topic will have the following formats.

        Command   = [subsystem tag]_command_[*[a-z][A-Z]]
        Event     = [subsystem tag]_logevent_[*[a-z][A-Z]]
        Telemetry = [subsystem tag]_[*[a-z][A-Z]]
        """

        if "command" in self.topic:
            self.is_command = True

        elif "logevent" in self.topic:
            self.is_event = True

        else:
            self.is_telemetry = True

        # 3) Tell the SAL manager to subscribe to the topic.
        # We currently are not considering Commands in this class.
        if self.is_command:
            raise ValueError("DDSSubsciber only considers Telemetry, or Event "
                             "topics. The given topic " + str(self.topic) +
                             "is a command")
        if self.is_event:
            self.mgr.salEvent(self.topic)

        if self.is_telemetry:
            self.mgr.salTelemetrySub(self.topic)

    def set_data(self):

        # Either is_event or is_telemetry must be True for this method to work.
        if self.is_event and self.is_telemetry and False:
            raise ValueError("There are improperly configured attributes, call "
                             "configure(). If this does not resolve the problem "
                             "file a bug report.")

        elif self.is_event:
            self.data = getattr(self.salpy_lib, "{}C".format(self.topic))()
        elif self.is_telemetry:
            self.data = getattr(self.salpy_lib, "{}C".format(self.topic))()

        else:
            raise ValueError("There are improperly configured attributes, call "
                             "configure(). If this does not resolve the problem "
                             "file a bug report.")

    def run(self):

        if self.is_event:
            self.run_event()

        if self.is_telemetry:
            self.run_telemetry()

    def run_event(self):

        self.getEvent = getattr(self.mgr,'getEvent_{}'.format(self.short_topic))

        while True:
            retval = self.getEvent(self.data)

            if retval == 0:

                # Get all the attributes of the self.data object
                # https://stackoverflow.com/questions/5969806/print-all-properties-of-a-python-class
                attributes = [attr for attr in dir(self.data) if not attr.startswith('__')]

                for attribute in attributes:
                    value = getattr(self.data, attribute)

                    self.handle[attribute] = value

            time.sleep(self.rate)

    def run_telemetry(self):

        self.getNextSample = getattr(self.mgr,"getNextSample_{}".format(self.short_topic))

        while True:
            retval = self.getNextSample(self.data)

            if retval ==0:

                # Get all the attributes of the self.data object
                # https://stackoverflow.com/questions/5969806/print-all-properties-of-a-python-class
                attributes = [attr for attr in dir(self.data) if not attr.startswith('__')]

                for attribute in attributes:
                    value = getattr(self.data, attribute)

                    self.handle[attribute] = value

            time.sleep(self.rate)

class DDSSubscriber(threading.Thread):

    ''' Class to Subscribe to Telemetry, it could a Command (discouraged), Event or Telemetry'''

    def __init__(self, Device, topic, threadID='1', Stype='Telemetry', tsleep=0.01, timeout=3600, nkeep=100):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.Device = Device
        self.topic = topic

        self.log = create_logger(name=self.Device)

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
        self.SALPY_lib = import_module('SALPY_{}'.format(self.Device))
        self.mgr = getattr(self.SALPY_lib, 'SAL_{}'.format(self.Device))()

        if self.Stype=='Telemetry':
            self.myData = getattr(self.SALPY_lib,'{}_{}C'.format(self.Device,self.topic))()
            self.mgr.salTelemetrySub("{}_{}".format(self.Device,self.topic))
            # Generic method to get for example: self.mgr.getNextSample_kernel_FK5Target
            self.getNextSample = getattr(self.mgr,"getNextSample_{}".format(self.topic))
            self.log.debug("{} subscriber ready for Device:{} topic:{}".format(self.Stype,self.Device,self.topic))
        elif self.Stype=='Event':
            self.myData = getattr(self.SALPY_lib,'{}_logevent_{}C'.format(self.Device,self.topic))()
            self.mgr.salEvent("{}_logevent_{}".format(self.Device,self.topic))
            # Generic method to get for example: self.mgr.getEvent_startIntegration(event)
            self.getEvent = getattr(self.mgr,'getEvent_{}'.format(self.topic))
            self.log.debug("{} subscriber ready for Device:{} topic:{}".format(self.Stype,self.Device,self.topic))
        elif self.Stype=='Command':
            self.myData = getattr(self.SALPY_lib,'{}_command_{}C'.format(self.Device,self.topic))()
            self.mgr.salProcessor("{}_command_{}".format(self.Device,self.topic))
            # Generic method to get for example: self.mgr.acceptCommand_takeImages(event)
            self.acceptCommand = getattr(self.mgr,'acceptCommand_{}'.format(self.topic))
            self.log.debug("{} subscriber ready for Device:{} topic:{}".format(self.Stype,self.Device,self.topic))

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
