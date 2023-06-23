import configparser
import logging
import os
from pathlib import Path

import appdirs  # type: ignore

import condorsmcstan

# PACKAGE DIRECTORIES
PACKAGE_ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PACKAGE_CONFIG_DIR = Path(
    appdirs.user_config_dir(appname="condorsmcstan", version=condorsmcstan.__version__)
)
PACKAGE_CACHE_DIR = Path(
    appdirs.user_cache_dir(appname="condorsmcstan", version=condorsmcstan.__version__)
)
PACKAGE_LOG_DIR = Path(
    appdirs.user_log_dir(appname="condorsmcstan", version=condorsmcstan.__version__)
)
PACKAGE_DATA_DIR = Path(
    appdirs.user_data_dir(appname="condorsmcstan", version=condorsmcstan.__version__)
)

# SESSION DIRECTORIES
SESSION_CONFIG_DIR = lambda session_id: Path(
    appdirs.user_config_dir(appname="condorsmcstan", version=condorsmcstan.__version__),
    session_id,
)
SESSION_CACHE_DIR = lambda session_id: Path(
    appdirs.user_cache_dir(appname="condorsmcstan", version=condorsmcstan.__version__),
    session_id,
)
SESSION_LOG_DIR = lambda session_id: Path(
    appdirs.user_log_dir(appname="condorsmcstan", version=condorsmcstan.__version__),
    session_id,
)
SESSION_DATA_DIR = lambda session_id: Path(
    appdirs.user_data_dir(appname="condorsmcstan", version=condorsmcstan.__version__),
    session_id,
)

# CONFIG FILE
LOCAL_CONFIG_DIR = Path(Path.cwd(), "config")

if Path(LOCAL_CONFIG_DIR, "config.cfg").is_file():
    logging.info(f"Found config file in current working directory")
    CONFIG_PATH = Path(LOCAL_CONFIG_DIR, "config.cfg")
elif Path(PACKAGE_CONFIG_DIR, "config.cfg").is_file():
    logging.info("Found config file in config directory")
    CONFIG_PATH = Path(PACKAGE_CONFIG_DIR, "config.cfg")
else:
    logging.info("Could not find config file in local or package directory")
    PACKAGE_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH = Path(PACKAGE_CONFIG_DIR, "config.cfg")
    with open(CONFIG_PATH, "w") as f:
        f.write(
            """[mysql]
                host = localhost
                user = root
                password =
                database = condorsmcstan
                poll_delay = 5
                """
        )
    raise (
        FileNotFoundError(
            f"Config file not found. Please edit {CONFIG_PATH} and try again."
        )
    )

CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

if not all(
    CONFIG.has_option("mysql", key) for key in ("host", "user", "password", "database")
):
    raise ValueError("Missing required database configuration information")

MYSQL_HOST = CONFIG.get("mysql", "host")
MYSQL_USER = CONFIG.get("mysql", "user")
MYSQL_PASSWORD = CONFIG.get("mysql", "password")
MYSQL_DATABASE = CONFIG.get("mysql", "database")
MYSQL_POLL_DELAY = (
    CONFIG.getint("mysql", "poll_delay")
    if CONFIG.has_option("mysql", "poll_delay")
    else 5
)
MYSQL_MAX_POLL_ATTEMPTS = (
    CONFIG.getint("mysql", "max_poll_attempts")
    if CONFIG.has_option("mysql", "max_poll_attempts")
    else 10
)

CONDORSMCSTAN_TICK_RATE = (
    CONFIG.getint("condorsmcstan", "tick_rate")
    if CONFIG.has_option("condorsmcstan", "tick_rate")
    else 5
)
CONDORSMCSTAN_OUTPUT_DIR = (
    Path(CONFIG.get("condorsmcstan", "output_dir"))
    if CONFIG.has_option("condorsmcstan", "output_dir")
    else Path(Path.cwd(), "output")
)
SESSION_OUTPUT_DIR = lambda session_id: Path(CONDORSMCSTAN_OUTPUT_DIR, session_id)
CONDORSMCSTAN_WORKER_TIMEOUT = (
    CONFIG.getint("condorsmcstan", "worker_timeout")
    if CONFIG.has_option("condorsmcstan", "worker_timeout")
    else 60
)
