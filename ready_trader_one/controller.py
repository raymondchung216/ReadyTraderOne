# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader One.
#
#     Ready Trader One is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader One is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader One.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import logging

from typing import Any, Optional

from .execution import ExecutionServer
from .heads_up import HeadsUpDisplayServer
from .information import InformationPublisher
from .market_events import MarketEventsReader
from .match_events import MatchEventsWriter
from .score_board import ScoreBoardWriter
from .timer import Timer


class Controller:
    """Controller for the Ready Trader One matching engine."""

    def __init__(self, loop: asyncio.AbstractEventLoop, market_open_delay: float, exec_server: ExecutionServer,
                 info_publisher: InformationPublisher, market_events_reader: MarketEventsReader,
                 match_events_writer: MatchEventsWriter, score_board_writer: ScoreBoardWriter, timer: Timer):
        """Initialise a new instance of the Controller class."""
        self.__done: bool = False
        self.__event_loop: asyncio.AbstractEventLoop = loop
        self.__execution_server: ExecutionServer = exec_server
        self.__heads_up_display_server: Optional[HeadsUpDisplayServer] = None
        self.__information_publisher: InformationPublisher = info_publisher
        self.__logger: logging.Logger = logging.getLogger("CONTROLLER")
        self.__market_events_reader = market_events_reader
        self.__market_open_delay: float = market_open_delay
        self.__match_events_writer = match_events_writer
        self.__score_board_writer = score_board_writer
        self.__timer: Timer = timer

        # Connect signals
        self.__match_events_writer.task_complete.append(self.on_task_complete)
        self.__market_events_reader.task_complete.append(self.on_task_complete)
        self.__score_board_writer.task_complete.append(self.on_task_complete)
        self.__timer.timer_stopped.append(self.on_timer_stopped)
        self.__timer.timer_ticked.append(self.on_timer_tick)

    def on_task_complete(self, task: Any) -> None:
        """Called when a reader or writer task is complete"""
        if task is self.__match_events_writer:
            self.__match_events_writer = None
        elif task is self.__score_board_writer:
            self.__score_board_writer = None
        elif task is self.__market_events_reader:
            self.__done = True

        if self.__match_events_writer is None and self.__score_board_writer is None:
            self.__event_loop.stop()

    def on_timer_stopped(self, timer: Timer, now: float) -> None:
        """Shut down the match."""
        self.__match_events_writer.finish()
        self.__score_board_writer.finish()

    def on_timer_tick(self, timer: Timer, now: float, _: int) -> None:
        """Called when it is time to send an order book update and trade ticks."""
        if self.__done:
            timer.shutdown(now, "match complete")
            return

    def set_heads_up_display_server(self, heads_up_display_server: HeadsUpDisplayServer) -> None:
        """Set the Heads Up Display server for this controller."""
        self.__heads_up_display_server = heads_up_display_server

    async def start(self) -> None:
        """Start running the match."""
        self.__logger.info("starting the match")

        await self.__execution_server.start()
        await self.__information_publisher.start()
        if self.__heads_up_display_server:
            await self.__heads_up_display_server.start()

        self.__market_events_reader.start()
        self.__match_events_writer.start()
        self.__score_board_writer.start()

        # Give the auto-traders time to start up and connect
        await asyncio.sleep(self.__market_open_delay)
        # self.__execution_server.close()

        self.__logger.info("market open")
        self.__timer.start()
