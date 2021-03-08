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
import importlib
import logging
import socket
import sys

from typing import Any, Dict

from .application import Application
from .base_auto_trader import BaseAutoTrader
from .util import create_datagram_endpoint


# From Python 3.8, the proactor event loop is used by default on Windows
if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def __validate_hostname(config, section, key):
    try:
        config[section][key] = socket.gethostbyname(config[section][key])
    except socket.error:
        raise Exception("Could not validate hostname in %s configuration" % section)


def __validate_json_object(config, section, required_keys, value_types):
    obj = config[section]
    if type(obj) is not dict:
        raise Exception("%s configuration should be a JSON object" % section)
    if any(k not in obj for k in required_keys):
        raise Exception("A required key is missing from the %s configuration" % section)
    if any(type(obj[k]) is not t for k, t in zip(required_keys, value_types)):
        raise Exception("Element of inappropriate type in %s configuration" % section)


def __config_validator(config):
    """Return True if the specified config is valid, otherwise raise an exception."""
    if type(config) is not dict:
        raise Exception("Configuration file contents should be a JSON object")
    if any(k not in config for k in ("Execution", "Information", "TeamName", "Secret")):
        raise Exception("A required key is missing from the configuration")

    __validate_json_object(config, "Execution", ("Host", "Port"), (str, int))
    __validate_json_object(config, "Information", ("Interface", "ListenAddress", "Port"), (str, str, int))

    __validate_hostname(config, "Execution", "Host")
    __validate_hostname(config, "Information", "Interface")
    __validate_hostname(config, "Information", "ListenAddress")

    if type(config["TeamName"]) is not str:
        raise Exception("TeamName has inappropriate type")
    if len(config["TeamName"]) < 1 or len(config["TeamName"]) > 50:
        raise Exception("TeamName must be at least one, and no more than fifty, characters long")

    if type(config["Secret"]) is not str:
        raise Exception("Secret has inappropriate type")
    if len(config["Secret"]) < 1 or len(config["Secret"]) > 50:
        raise Exception("Secret must be at least one, and no more than fifty, characters long")

    return True


async def __start_autotrader(auto_trader: BaseAutoTrader, config: Dict[str, Any],
                             loop: asyncio.AbstractEventLoop) -> None:
    """Initialise an auto-trader."""
    logger = logging.getLogger("INIT")

    info = config["Information"]
    await create_datagram_endpoint(loop, lambda: auto_trader, (info["ListenAddress"], info["Port"]),
                                   family=socket.AF_INET, interface=info["Interface"])

    exec_ = config["Execution"]
    try:
        await loop.create_connection(lambda: auto_trader, exec_["Host"], exec_["Port"])
    except OSError as e:
        logger.error("execution connection failed: %s", e.strerror)
        loop.stop()
        return


def main(name: str = "autotrader") -> None:
    """Import the 'AutoTrader' class from the named module a run it."""
    app = Application(name, __config_validator)

    mod = importlib.import_module(name)
    auto_trader = mod.AutoTrader(app.event_loop, app.config["TeamName"], app.config["Secret"])

    app.event_loop.create_task(__start_autotrader(auto_trader, app.config, app.event_loop))
    app.run()
