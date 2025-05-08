"""
Queue implementation for MCP responses
"""

import asyncio
import logging
from collections import defaultdict
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
    An in-memory queue implementation for MCP responses.
    Each session has its own asyncio.Queue.
    """

    def __init__(self):
        """
        Initialize the in-memory queue.
        Uses a dictionary of asyncio.Queues for session management.
        """
        self.queues = defaultdict(asyncio.Queue)

    def push_response(
        self,
        session_id: str,
        response: MCPResponse,
    ) -> None:
        """
        Push a response to the in-memory queue for a specific session.
        """
        logger.debug(f"InMemoryQueue: Pushing response for session: {session_id}")
        queue = self.queues[session_id]
        asyncio.create_task(queue.put(response.model_dump_json(exclude_none=True)))

    async def wait_for_response(
        self, session_id: str, timeout: float | None = None
    ) -> str | None:
        """
        Wait for a response from the in-memory queue for a specific session.
        """
        queue = self.queues[session_id]
        try:
            if timeout is not None:
                response = await asyncio.wait_for(queue.get(), timeout)
            else:
                response = await queue.get()
            logger.debug(f"InMemoryQueue: Retrieved response for session: {session_id}")
            return response
        except asyncio.TimeoutError:
            logger.warning(f"InMemoryQueue: Timeout waiting for response in session: {session_id}")
            return None

    def clear_session(self, session_id: str) -> None:
        """
        Clear all queued responses for a session.
        """
        logger.debug(f"InMemoryQueue: Clearing session: {session_id}")
        if session_id in self.queues:
            self.queues[session_id] = asyncio.Queue()