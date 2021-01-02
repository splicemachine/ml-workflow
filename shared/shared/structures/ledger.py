"""
Defines the Job Ledger
necessary for jobs being
serviced only once
"""

from collections import deque

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Ben Epstein"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class JobLedger:
    """
    A data structure that fills up
    to a maximum size with elements
    and then starts deleting oldest elements
    to maintain this size.

    *We use this to provide a buffer for jobs
    that are taking a while (especially if
    Splice Machine DB is running slow). We
    do not want to EVER service the same job
    from multiple threads (as it is probable
    to produce a write-write conflict). Thus,
    if a Job hasn't been updated to RUNNING
    status by the thread in time for the next
    poll, the Job ledger will make sure that it
    isn't serviced again. Obviously, this task
    could be done with a list, but this data
    structure is better-- it doesn't require
    searching through hundreds of jobs to find
    if one has been serviced yet. We use a deque
    (implemented as a linked list) because it
    has efficient deleting at head: O(1)*
    """

    def __init__(self, max_size: int) -> None:
        """
        :param max_size: (int) the maximum size of the
            list
        """
        self.max_size: int = max_size

        self.current_size: int = 0  # so we don't need to
        # keep finding length to improve performance

        self._linked_list: deque = deque()

    def record(self, job_id: int) -> None:
        """
        Record a new job_id in the job ledger,
        maintaining the maximum size

        :param job_id: (int) the job id to
            record in the list

        """
        if self.current_size >= self.max_size:
            self._linked_list.popleft()
            self.current_size -= 1

        self._linked_list.append(job_id)
        self.current_size += 1

    def __contains__(self, job_id: int) -> bool:
        """
        Return whether or not the Job Ledger
        contains a given job_id

        :param job_id: (int) the job id to check
            for the existence of
        :return: (bool) whether or not the job ledger
            contains the specified job id
        """
        return job_id in self._linked_list
