#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Celery command."""
from __future__ import annotations

from contextlib import contextmanager
from multiprocessing import Process

import psutil
import json
import subprocess

from kafka import KafkaConsumer
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile

from airflow import settings, exceptions
from airflow.configuration import conf
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations
from airflow.utils.serve_logs import serve_logs
from airflow.utils.state import State

WORKER_PROCESS_NAME = "worker"

@contextmanager
def _serve_logs(skip_serve_logs: bool = False):
    """Starts serve_logs sub-process."""
    sub_proc = None
    if skip_serve_logs is False:
        sub_proc = Process(target=serve_logs)
        sub_proc.start()
    yield
    if sub_proc:
        sub_proc.terminate()


@cli_utils.action_cli
def worker(args):
    """Starts Airflow Kafka worker."""
    settings.reconfigure_orm(disable_connection_pool=True)
    if not settings.validate_session():
        raise SystemExit("Worker exiting, database connection precheck failed.")

    bootstrap_servers = conf.get("kafka", "bootstrap_servers")

    if not args.queues:
        msg = f"KafkaExecutor needs at least 1 queue specified."
        raise exceptions.AirflowException(msg)
    consumer = KafkaConsumer(args.queues, bootstrap_servers=bootstrap_servers, group_id="test", auto_offset_reset="earliest")
    for msg in consumer:
        msgValue = json.loads(msg.value)
        subprocess.check_call(msgValue, close_fds=True)

@cli_utils.action_cli
def stop_worker(args):
    """Sends SIGTERM to Celery worker."""
    # Read PID from file
    if args.pid:
        pid_file_path = args.pid
    else:
        pid_file_path, _, _, _ = setup_locations(process=WORKER_PROCESS_NAME)
    pid = read_pid_from_pidfile(pid_file_path)

    # Send SIGTERM
    if pid:
        worker_process = psutil.Process(pid)
        worker_process.terminate()

    # Remove pid file
    remove_existing_pidfile(pid_file_path)
