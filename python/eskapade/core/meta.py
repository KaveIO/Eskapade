"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/09/14

Description:

    A collection of (generic) meta classes for some (design) patterns:

        - Singleton: Meta class for the Singleton pattern.
        - Processor: Meta class with abstract methods initialize,
          execute, and finalize.
        - ProcessorSequence: A simple (processor) sequence container.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from abc import ABCMeta, abstractmethod
from weakref import proxy

from eskapade.core.definitions import StatusCode
from eskapade.logger import Logger


class Singleton(type):
    """Metaclass for singletons.

    Any instantiation of a Singleton class yields the exact same object, e.g.:

    >>> class Klass(metaclass=Singleton):
    >>>     pass
    >>>
    >>> a = Klass()
    >>> b = Klass()
    >>> a is b
    True

    See https://michaelgoerz.net/notes/singleton-objects-in-python.html.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """Return the singleton."""
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]

    @classmethod
    def __instancecheck__(mcs, instance) -> bool:
        if instance.__class__ is mcs:
            return True
        return isinstance(instance.__class__, mcs)


class Processor(metaclass=ABCMeta):
    """Processor metaclass."""

    logger = Logger()  # type: Logger

    def __init__(self, name: str):
        """Initialize the Processor object."""
        super().__init__()
        name = name or self.__class__.__name__
        self.__name = name  # type: str
        self.__hash = None  # type: int
        self.__parent = None

    def __str__(self) -> str:
        return self.__name

    def __repr__(self) -> str:
        return '<{klass!s} name={name!s} parent={parent!r} id={id!s}>'.format(klass=self.__class__.__name__,
                                                                              name=self.name,
                                                                              parent=self.__parent,
                                                                              id=id(self))

    def __eq__(self, other: 'Processor') -> bool:
        return isinstance(other, type(self)) and self.__name == other.__name

    def __hash__(self) -> int:
        if self.__hash is None:
            self.__hash = hash((type(self), self.__name))

        return self.__hash

    def _initialize(self):
        """Wrapper to call user implemented initialize."""
        self.logger.debug('Initializing link "{link!s}".', link=self)

        status = self.initialize()

        if status == StatusCode.Success:
            self.logger.debug('Successfully initialized link "{link!s}".', link=self)

        return status

    @abstractmethod
    def initialize(self):
        """Initialization logic for processor."""
        raise NotImplementedError

    def _execute(self):
        """Wrapper to call user implemented execute."""
        self.logger.debug('Executing link "{link!s}".', link=self)

        status = self.execute()

        if status == StatusCode.Success:
            self.logger.debug('Successfully executed link "{link!s}".', link=self)

        return status

    @abstractmethod
    def execute(self):
        """Execution logic for processor."""
        raise NotImplementedError

    def _finalize(self):
        """Wrapper to call user implemented finalize."""
        self.logger.debug('Finalizing link "{link!s}".', link=self)

        status = self.finalize()

        if status == StatusCode.Success:
            self.logger.debug('Successfully finalized link "{link!s}".', link=self)

        return status

    @abstractmethod
    def finalize(self):
        """Finalization logic for processor."""
        raise NotImplementedError

    @property
    def name(self) -> str:
        """Get the name of processor.

        :return: The name of the processor.
        :rtype: str
        """
        return self.__name

    @property
    def parent(self):
        """Get the group parent.

        :return: The parent/group processor sequence.
        """
        return self.__parent

    @parent.setter
    def parent(self, the_parent) -> None:
        """Set the group parent.

        :param the_parent: The parent/group processor sequence.
        """
        self.__parent = None

        if the_parent is not None:
            # The parent will most likely outlive the processor
            # and therefore we do not want keep a strong reference
            # to the parent.
            self.__parent = proxy(the_parent)


class _ProcessorNode(object):
    """A Processor Node.

    This is used by :class:`ProcessingSequence` to doubly link processors that are
    to be executed sequentially.

    :attr prev: The previous processor in the chain.
    :attr next: The next processor in the chain.
    :attr value: The current processor.
    """

    __slots__ = 'prev', 'next', 'value', '__weakref__'

    def __init__(self):
        """Initialize the ProcessorNode object."""
        self.prev = None  # type: Processor
        self.next = None  # type: Processor
        self.value = None  # type: Processor


class ProcessorSequence(object):
    """A doubly linked processor sequence.

    It remembers the order in which processors are added to the sequence.
    It also checks if a processor already has been added to the sequence.
    """

    __slots__ = '__end', '__map', '__weakref__'

    def __init__(self):
        """Initialize the ProcessorSequence object."""
        super().__init__()
        self.__end = end = _ProcessorNode()  # type: _ProcessorNode
        end.prev = end.next = end  # type: _ProcessorNode
        self.__map = {}

    def __len__(self) -> int:
        """Get the length, i.e. the number of processors.

        :return: The length of the sequence.
        :rtype: int
        """
        return len(self.__map)

    def __contains__(self, processor: Processor) -> bool:
        """Check if sequence contains processor.

        :param processor: The processor to check.
        :type processor: Processor
        :return: True when the sequence contains the processor otherwise False.
        :rtype: bool
        """
        return processor in self.__map

    def __iter__(self):
        """Forward iterator.

        :return: Generator object.
        """
        end = self.__end
        curr_node = end.next
        while curr_node is not end:
            yield curr_node.value
            curr_node = curr_node.next

    def __reversed__(self):
        """Reverse iterator.

        :return: Generator object.
        """
        end = self.__end
        curr_node = end.prev
        while curr_node is not end:
            yield curr_node.value
            curr_node = curr_node.prev

    def __repr__(self) -> str:
        return '<{klass!s} id={id!s} sequence={repr!s}>'.format(klass=self.__class__.__name__,
                                                                id=id(self),
                                                                repr=set(self) if self else '{}')

    def add(self, processor: Processor) -> None:
        """Add a processor to the sequence.

        :param processor: The processor to add.
        :type processor: Processor
        :raise KeyError: When a processor of the same type and name already exists.
        """
        if processor not in self.__map:
            self.__map[processor] = node = _ProcessorNode()
            end = self.__end
            last = end.prev
            node.prev, node.next, node.value = last, end, processor
            last.next = end.prev = proxy(node)
        else:
            raise KeyError('Processor "{processor!r}" already exists!'.format(processor=processor))

    def discard(self, processor: Processor) -> None:
        """Remove a processor from the sequence.

        :param processor: The processor to remove.
        :type processor: Processor
        :raise KeyError: When the processor does not exist.
        """
        if processor in self.__map:
            node = self.__map.pop(processor)
            node.prev.next = node.next
            node.next.prev = node.prev
        else:
            raise KeyError('Unknown processor "{processor!r} to discard"!'.format(processor=processor))

    def pop(self, last: bool = True) -> Processor:
        """Return the popped processor. Raise KeyError if empty.

        By default a processor is popped from the end of the sequence.

        :param last: Pop processor from the end of the sequence. Default is True.
        :type last: bool
        :return: The pop processor.
        :raise KeyError: When trying to pop from an empty list.
        """
        if not self:
            raise KeyError('Set is empty!')
        # noinspection PyTypeChecker
        processor = next(reversed(self)) if last else next(iter(self))
        self.discard(processor)
        return processor

    def clear(self) -> None:
        """Clear the sequence."""
        # Reset end node.
        self.__end.prev = self.__end.next = self.__end
        # Clear processor to processor node map.
        self.__map.clear()
