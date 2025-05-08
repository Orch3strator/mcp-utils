"""
Queue implementation for MCP responses
"""

import queue
import logging
from collections import defaultdict, deque
from threading import Lock
from typing import Protocol

from .schema import MCPResponse

logger = logging.getLogger("mcp_utils")


class ResponseQueueProtocol(Protocol):
    """Protocol defining the interface for response queues"""

    def push_response(
        self,
        session_id: str,
        response: MCPResponse,
    ) -> None: ...

    def wait_for_response(
        self, session_id: str, timeout: float | None = None
    ) -> str | None: ...

    def clear_session(self, session_id: str) -> None: ...


class RedisResponseQueue(ResponseQueueProtocol):
    """
    A Redis-backed queue implementation for MCP responses.
    Each session has its own Redis list.
    """

    def __init__(self, redis_client):
        """
        Initialize Redis queue

        Args:
            redis_client: Redis client instance
        """
        self.redis = redis_client

    def _get_queue_key(self, session_id: str) -> str:
        """Get Redis key for session queue"""
        return f"mcp:response_queue:{session_id}"

    def push_response(
        self,
        session_id: str,
        response: MCPResponse,
    ) -> None:
        """
        Push a response to the Redis queue for a specific session

        Args:
            session_id: The session ID
            response: The response to push
        """
        queue_key = self._get_queue_key(session_id)
        value = response.model_dump_json(exclude_none=True)
        logger.debug(f"Redis: Saving response for session: {session_id}: {value}")
        self.redis.rpush(queue_key, value)

    def wait_for_response(
        self, session_id: str, timeout: float | None = None
    ) -> MCPResponse | None:
        """
        Wait for a response from the Redis queue for a specific session

        Args:
            session_id: The session ID
            timeout: How long to wait for a response in seconds.
                    If None, wait indefinitely.
                    If 0, return immediately if no response is available.

        Returns:
            The next queued response or None if timeout occurs
        """
        queue_key = self._get_queue_key(session_id)
        if timeout == 0:
            # Non-blocking check
            data = self.redis.lpop(queue_key)
        else:
            # Blocking wait with timeout
            data = self.redis.blpop(
                queue_key, timeout=timeout if timeout is not None else 0
            )
            if data:
                # blpop returns (key, value) tuple
                data = data[1]

        if not data:
            return None
        elif isinstance(data, bytes):
            return data.decode("utf-8")
        return data

    def clear_session(self, session_id: str) -> None:
        """
        Clear all queued responses for a session

        Args:
            session_id: The session ID to clear
        """
        queue_key = self._get_queue_key(session_id)
        self.redis.delete(queue_key)
        logger.debug(f"Redis: Clearing session: {session_id}")

class InMemoryResponseQueue(ResponseQueueProtocol):
    """
    Synchronous, thread-safe in-memory queue for MCP responses.
    Each session has its own queue with a max size of 1000.
    Oldest responses are dropped if full.
    """
    def __init__(self, max_queue_size=1000):
        # Use deque for fast append/pop and maxlen for auto-drop
        self.queues = defaultdict(lambda: deque(maxlen=max_queue_size))
        self.locks = defaultdict(Lock)
        self.max_queue_size = max_queue_size

    def push_response(self, session_id: str, response: MCPResponse) -> None:
        with self.locks[session_id]:
            self.queues[session_id].append(response.model_dump_json(exclude_none=True))
            # If the queue is full, the oldest is automatically dropped by deque

    def wait_for_response(self, session_id: str, timeout: float | None = None) -> str | None:
        import time
        deadline = time.time() + timeout if timeout else None
        while True:
            with self.locks[session_id]:
                if self.queues[session_id]:
                    return self.queues[session_id].popleft()
            if timeout is not None and time.time() > deadline:
                return None
            time.sleep(0.05)  # Avoid busy waiting

    def clear_session(self, session_id: str) -> None:
        with self.locks[session_id]:
            if session_id in self.queues:
                del self.queues[session_id]
            if session_id in self.locks:
                del self.locks[session_id]
