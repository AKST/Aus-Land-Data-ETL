from abc import ABC
from dataclasses import dataclass

class ParentMessage:
    @dataclass
    class T(ABC):
        process_index: int
        worker_index: int

    @dataclass
    class Queued(T):
        """
        A child will send this this to the parent
        when it's been queued a number of property
        descriptions. This typically happens at the
        start.
        """
        amount: int

    @dataclass
    class Processed(T):
        """
        A child will send this this to the parent
        when it's processed a number of property
        descriptions.
        """
        amount: int
