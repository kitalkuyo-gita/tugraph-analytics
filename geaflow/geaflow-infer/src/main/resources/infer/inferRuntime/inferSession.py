#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import logging
import os
from typing import Any, Tuple

import torch

from baseInferSession import BaseInferSession


def _resolve_torch_threads() -> int:
    try:
        env_value = os.environ.get("GEAFLOW_TORCH_NUM_THREADS")
        return int(env_value) if env_value else 1
    except ValueError:
        logging.getLogger(__name__).warning(
            "Invalid GEAFLOW_TORCH_NUM_THREADS=%s, fallback to 1", env_value
        )
        return 1


class TorchInferSession(BaseInferSession):
    def __init__(self, transform_class) -> None:
        super().__init__(transform_class)
        torch.set_num_threads(_resolve_torch_threads())
        self._model = getattr(self.transform, "model", None)

    def run(self, *inputs):
        preprocess_output = self._call_transform_pre(*inputs)
        if callable(self._model):
            args, kwargs = self._normalize_arguments(preprocess_output)
            with torch.no_grad():
                result = self._model(*args, **kwargs)
        else:
            result = self._extract_passthrough(preprocess_output)
        return self._call_transform_post(result)
