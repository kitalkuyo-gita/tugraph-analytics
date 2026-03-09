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
PaddlePaddle Inference Session

This module provides PaddlePaddle-based inference session support,
including both dynamic graph (paddle.jit.load) and static graph
(paddle.inference.create_predictor) execution modes.
"""

import logging
import os

import numpy as np
import paddle

from baseInferSession import BaseInferSession


def _resolve_paddle_threads() -> int:
    """Resolve number of threads for PaddlePaddle."""
    try:
        env_value = os.environ.get("GEAFLOW_PADDLE_NUM_THREADS")
        return int(env_value) if env_value else 1
    except ValueError:
        logging.getLogger(__name__).warning(
            "Invalid GEAFLOW_PADDLE_NUM_THREADS=%s, fallback to 1", env_value
        )
        return 1


class PaddleInferSession(BaseInferSession):
    """
    PaddlePaddle inference session implementation.
    
    Supports two modes:
    1. Dynamic graph mode (paddle.jit.load) - for development/debugging
    2. Static graph mode (paddle.inference) - for production deployment
    """

    def __init__(self, transform_class) -> None:
        """
        Initialize PaddlePaddle inference session.
        
        Args:
            transform_class: User-defined transformation class with load_model(),
                           transform_pre(), and transform_post() methods
        """
        super().__init__(transform_class)
        
        # Set thread count
        paddle.set_num_threads(_resolve_paddle_threads())
        
        # Determine inference mode
        self._infer_mode = getattr(transform_class, "infer_mode", "dynamic")
        self._model = None
        self._predictor = None
        
        # Load model based on mode
        if hasattr(transform_class, 'model'):
            self._model = transform_class.model
        elif hasattr(transform_class, 'load_model'):
            # Model already loaded by user in __init__
            pass
        
        # Setup predictor for static graph mode
        if self._infer_mode == "static":
            self._setup_static_predictor()

    def _setup_static_predictor(self):
        """Setup Paddle Inference predictor for static graph execution."""
        try:
            model_dir = getattr(self._transform, 'model_dir', None)
            if model_dir is None:
                raise ValueError("model_dir must be provided for static inference mode")
            
            config = paddle.inference.Config(model_dir + ".pdmodel", 
                                            model_dir + ".pdiparams")
            
            # Enable GPU if available
            if paddle.device.is_compiled_with_cuda() and paddle.device.cuda.device_count() > 0:
                config.enable_use_gpu(200, 0)
            else:
                config.disable_gpu()
            
            # Enable MKLDNN for CPU optimization
            if not paddle.device.is_compiled_with_cuda():
                config.enable_mkldnn()
            
            self._predictor = paddle.inference.create_predictor(config)
            logging.getLogger(__name__).info("Static graph predictor created successfully")
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Failed to create static predictor: {e}")
            raise

    def run(self, *inputs):
        """
        Execute PaddlePaddle inference.
        
        This method:
        1. Calls transform_pre() to preprocess inputs
        2. Converts Paddle tensors to numpy for pickle compatibility
        3. Runs model inference (dynamic or static mode)
        4. Calls transform_post() to post-process results
        
        Args:
            *inputs: Variable number of input arguments
            
        Returns:
            Inference results after post-processing
        """
        try:
            # Preprocess
            preprocess_output = self._call_transform_pre(*inputs)
            
            if self._infer_mode == "dynamic" and callable(self._model):
                # Dynamic graph mode
                args, kwargs = self._normalize_arguments(preprocess_output)
                
                # Convert numpy arrays to Paddle tensors if needed
                args = self._to_paddle_tensors(args)
                kwargs = {k: self._to_paddle_tensor(v) for k, v in kwargs.items()}
                
                with paddle.no_grad():
                    result = self._model(*args, **kwargs)
                    
            elif self._infer_mode == "static" and self._predictor is not None:
                # Static graph mode
                result = self._run_static_inference(preprocess_output)
                
            else:
                # Fallback: use preprocess output directly
                result = self._extract_passthrough(preprocess_output)
            
            # Postprocess
            return self._call_transform_post(result)
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Paddle inference failed: {e}")
            raise

    def _run_static_inference(self, preprocess_output):
        """
        Run inference using Paddle Inference static predictor.
        
        Args:
            preprocess_output: Output from transform_pre
            
        Returns:
            Inference result
        """
        try:
            # Get input names from predictor
            input_names = self._predictor.get_input_names()
            output_names = self._predictor.get_output_names()
            
            # Prepare inputs
            args, kwargs = self._normalize_arguments(preprocess_output)
            
            # Handle different input scenarios
            if len(input_names) == 1:
                # Single input model
                input_tensor = self._predictor.get_input_handle(input_names[0])
                input_data = self._to_numpy(args[0] if args else kwargs.get('data'))
                input_tensor.copy_from_cpu(input_data)
            else:
                # Multiple inputs - match by name
                for i, name in enumerate(input_names):
                    input_tensor = self._predictor.get_input_handle(name)
                    if i < len(args):
                        input_data = self._to_numpy(args[i])
                    elif name in kwargs:
                        input_data = self._to_numpy(kwargs[name])
                    else:
                        raise ValueError(f"Missing input for {name}")
                    input_tensor.copy_from_cpu(input_data)
            
            # Run inference
            self._predictor.run()
            
            # Get outputs
            results = []
            for name in output_names:
                output_tensor = self._predictor.get_output_handle(name)
                output_data = output_tensor.copy_to_cpu()
                results.append(output_data)
            
            return tuple(results) if len(results) > 1 else results[0]
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Static inference failed: {e}")
            raise

    def _to_paddle_tensor(self, data):
        """Convert numpy array or list to Paddle tensor."""
        if isinstance(data, paddle.Tensor):
            return data
        if isinstance(data, (np.ndarray, np.generic)):
            return paddle.to_tensor(data)
        if isinstance(data, (list, tuple)):
            return paddle.to_tensor(np.array(data))
        return data

    def _to_paddle_tensors(self, args_tuple):
        """Convert tuple of numpy arrays to Paddle tensors."""
        return tuple(self._to_paddle_tensor(arg) for arg in args_tuple)

    def _to_numpy(self, data):
        """Convert Paddle tensor to numpy array."""
        if isinstance(data, paddle.Tensor):
            return data.numpy()
        if isinstance(data, np.ndarray):
            return data
        if isinstance(data, (list, tuple)):
            return np.array(data)
        return np.array([data])
