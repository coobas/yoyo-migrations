# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Handle config file and argument parsing
"""
import os
from yoyo.compat import SafeConfigParser

CONFIG_FILENAME = '.yoyorc'


def update_argparser_defaults(parser, defaults):
    """
    Update an ArgumentParser's defaults.

    Unlike ArgumentParser.set_defaults this will only set defaults for
    arguments the parser has configured.
    """
    ns, _ = parser.parse_known_args([])
    parser.set_defaults(**{k: v
                            for k, v in defaults.items()
                            if k in ns.__dict__})


def read_config(path):
    """
    Read the configuration file at ``path``, or return an empty
    ConfigParse object if ``path`` is ``None``.
    """
    if path is None:
        return SafeConfigParser()
    config = SafeConfigParser()
    config.read([path])
    return config


def save_config(config, path):
    """
    Write the configuration file to ``path``.
    """
    os.umask(0o77)
    f = open(path, 'w')
    try:
        return config.write(f)
    finally:
        f.close()


def find_config():
    """Find the closest config file in the cwd or a parent directory"""
    d = os.getcwd()
    while d != os.path.dirname(d):
        path = os.path.join(d, CONFIG_FILENAME)
        if os.path.isfile(path):
            return path
        d = os.path.dirname(d)
    return None
