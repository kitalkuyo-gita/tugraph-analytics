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
Base Infer Session - Framework Agnostic Abstract Base Class

This module defines the abstract interface for all inference sessions,
ensuring framework-independent execution in GeaFlow.
"""

from abc import ABC, abstractmethod


class BaseInferSession(ABC):
    """
    Abstract base class for inference sessions.
    
    All framework-specific session implementations (Torch, Paddle, etc.)
    must inherit from this class and implement the run() method.
    """

    def __init__(self, transform_class):
        """
        Initialize the inference session.
        
        Args:
            transform_class: User-defined transformation class that implements
                           load_model(), transform_pre(), and transform_post()
        """
        self._transform = transform_class

    @abstractmethod
    def run(self, *inputs):
        """
        Execute inference on given inputs.
        
        This method should:
        1. Call transform_pre() to preprocess inputs
        2. Run model inference
        3. Call transform_post() to post-process results
        
        Args:
            *inputs: Variable number of input arguments
            
        Returns:
            Inference results after post-processing
        """
        pass
