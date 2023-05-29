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
"""
DaskExecutor.

.. seealso::
    For more information on how the DaskExecutor works, take a look at the guide:
    :ref:`executor:DaskExecutor`
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from kafka import KafkaProducer

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstancekey import TaskInstanceKey


# queue="default" is a special case since this is the base config default queue name,
# with respect to DaskExecutor, treat it as if no queue is provided
_UNDEFINED_QUEUES = {None, "default"}


class KafkaExecutor(BaseExecutor):
    """KafkaExecutor submits tasks to a Kafka Topic.
    This can be used in combination with a kafka airflow worker.

    In the KafkaExecutor pattern, each worker consumes from the Kafka Topic which matches the 'queue' parameter.
    Each task gets sent to this queue, so the worker consumes it and executes the task similarly to the Celery Worker pattern.
    """

    supports_pickling: bool = False

    def __init__(self, bootstrap_servers=None):
        super().__init__(parallelism=0)
        if bootstrap_servers is None:
            bootstrap_servers = conf.get("kafka", "bootstrap_servers")
        if not bootstrap_servers:
            raise ValueError("Please provide a Dask cluster address in airflow.cfg")
        self.bootstrap_servers = bootstrap_servers
        self._producer = None

    def start(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            #security_protocol="SSL",
            #ssl_cafile=folderName + "ca.pem",
            #ssl_certfile=folderName + "service.cert",
            #ssl_keyfile=folderName + "service.key",
            #value_serializer=lambda v: json.dumps(v).encode('ascii'),
            #key_serializer=lambda v: json.dumps(v).encode('ascii')

        )

    def _process_tasks(self, task_tuples: list) -> None:
        task_tuples_to_send = [task_tuple[:3] for task_tuple in task_tuples]
        for task_tuple in task_tuples_to_send:
            key, command, queue = task_tuple
            self._producer.send(queue, json.dumps(command).encode())
            self._producer.flush()
            self.queued_tasks.pop(key)

    def end(self) -> None:
        pass

    def terminate(self):
        pass
