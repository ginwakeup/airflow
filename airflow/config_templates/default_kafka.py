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
"""Default kafka configuration."""
from __future__ import annotations

import logging
import ssl

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException

log = logging.getLogger(__name__)

bootstrap_servers = conf.get("kafka", "BOOTSTRAP_SERVERS")

DEFAULT_KAFKA_CONFIG = {
    "bootstrap_servers": bootstrap_servers
}

kafka_ssl_active = False
try:
    kafka_ssl_active = conf.getboolean("kafka", "SSL_ACTIVE")
except AirflowConfigException:
    log.warning("Kafka Executor will run without SSL")

try:
    if kafka_ssl_active:
        # TODO: implement ssl connection to Kafka.
        DEFAULT_KAFKA_CONFIG["kafka_use_ssl"] = None
except AirflowConfigException:
    raise AirflowException(
        "AirflowConfigException: SSL_ACTIVE is True, "
        "please ensure SSL_KEY, "
        "SSL_CERT and SSL_CACERT are set"
    )
except Exception as e:
    raise AirflowException()
