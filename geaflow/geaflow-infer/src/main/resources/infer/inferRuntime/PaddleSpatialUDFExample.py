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
PaddleSpatial UDF Example - Graph Node Classification

This example demonstrates how to implement a PaddlePaddle-based 
Graph Neural Network (GNN) UDF for node classification using PGL.

Key differences from PyTorch version:
1. Use paddle.Tensor instead of torch.Tensor
2. Use pgl.Graph instead of torch_geometric.data.Data
3. Convert tensors to numpy before pickle serialization
4. Support both dynamic and static graph inference modes
"""

import numpy as np
import paddle
import pgl
from TransFormFunction import TransFormFunction


class PaddleGCNTransform(TransFormFunction):
    """
    PaddlePaddle GCN model transformation function for node classification.
    
    This class demonstrates:
    - Loading PaddlePaddle GNN models
    - Building PGL graph objects
    - Converting between Paddle tensors and numpy arrays
    - Production-ready inference with static graph support
    """

    def __init__(self):
        """Initialize the transformation function."""
        # Call parent constructor with input size (number of inputs expected)
        super().__init__(1)
        print("Initializing PaddleGCNTransform")
        
        # Set device (GPU if available, otherwise CPU)
        self.device = 'gpu' if paddle.device.is_compiled_with_cuda() else 'cpu'
        print(f"Using device: {self.device}")
        
        # Load dataset configuration (Cora example)
        self.num_node_features = 1433  # Cora feature dimension
        self.num_classes = 7  # Cora number of classes
        self.hidden_dim = 16
        
        # Load pre-trained model
        self.load_model('model.pdparams')
        
        # Optional: Set inference mode ("dynamic" or "static")
        # self.infer_mode = "dynamic"  # For development
        self.infer_mode = "static"  # For production (faster)
        
        # For static inference, provide model directory
        # self.model_dir = "/path/to/model"

    def load_model(self, model_path: str):
        """
        Load pre-trained PaddlePaddle model.
        
        Args:
            model_path: Path to model parameters file (.pdparams)
                       or model directory for static inference
        """
        try:
            if self.infer_mode == "dynamic":
                # Dynamic graph mode - load model directly
                print(f"Loading model in dynamic mode from {model_path}")
                
                # Define GCN model architecture
                self.model = GCN(self.num_node_features, self.hidden_dim, self.num_classes)
                
                # Load parameters
                state_dict = paddle.load(model_path)
                self.model.set_state_dict(state_dict)
                self.model.eval()
                
            else:
                # Static graph mode - use Paddle Inference
                print(f"Setting up static inference from {model_path}")
                self.model_dir = model_path.replace('.pdparams', '')
                # Model will be loaded by PaddleInferSession
                
        except Exception as e:
            print(f"Error loading model: {e}")
            raise

    def transform_pre(self, *args):
        """
        Preprocess input before inference.
        
        This method:
        1. Receives vertex/edge data from GeaFlow
        2. Constructs PGL graph object with features
        3. Converts to numpy for pickle serialization
        
        Args:
            *args: Input arguments (e.g., vertex ID, features)
            
        Returns:
            Tuple of (graph_data_numpy, node_features_numpy)
        """
        try:
            vertex_id = args[0]
            
            # In a real scenario, you would:
            # 1. Fetch neighborhood from graph storage
            # 2. Build subgraph around vertex_id
            # 3. Extract node features
            
            # Example: Dummy subgraph construction
            # In practice, retrieve from graph context
            edges = [(0, 1), (1, 2), (2, 3), (3, 4)]  # Example edges
            num_nodes = 5
            
            # Create PGL graph
            graph = pgl.Graph(
                num_nodes=num_nodes,
                edges=edges,
                node_feat={
                    'feat': np.random.randn(num_nodes, self.num_node_features).astype('float32')
                }
            )
            
            # Get target node features
            node_features = graph.node_feat['feat'][0:1]  # Shape: [1, feature_dim]
            
            # IMPORTANT: Convert Paddle/numpy to plain numpy for pickle
            # Paddle tensors cannot be pickled directly
            if isinstance(node_features, paddle.Tensor):
                node_features = node_features.numpy()
            
            # Return as tuple (will be pickled and sent to Python subprocess)
            return (graph, node_features), node_features
            
        except Exception as e:
            print(f"Error in transform_pre: {e}")
            raise

    def transform_post(self, result):
        """
        Post-process inference results.
        
        This method:
        1. Receives raw model output
        2. Converts to Python list/dict
        3. Returns structured result for GeaFlow
        
        Args:
            result: Raw inference output (numpy array or Paddle tensor)
            
        Returns:
            List of [probability, predicted_class]
        """
        try:
            # Convert Paddle tensor to numpy if needed
            if isinstance(result, paddle.Tensor):
                result = result.numpy()
            
            # Compute probabilities and predictions
            if len(result.shape) > 1:
                # Multi-class classification
                probs = self._softmax(result[0])
                predicted_class = int(np.argmax(probs))
                max_prob = float(probs[predicted_class])
            else:
                # Binary classification or regression
                predicted_class = int(result[0] > 0.5) if len(result) == 1 else int(np.argmax(result))
                max_prob = float(result[predicted_class]) if len(result) > 1 else float(result[0])
            
            # Return as list for Java side consumption
            return [max_prob, predicted_class]
            
        except Exception as e:
            print(f"Error in transform_post: {e}")
            raise

    def _softmax(self, x):
        """Compute softmax values for each set of scores in x."""
        exp_x = np.exp(x - np.max(x, axis=-1, keepdims=True))
        return exp_x / np.sum(exp_x, axis=-1, keepdims=True)


class GCN(pgl.layers.Model):
    """
    Simple 2-layer Graph Convolutional Network for node classification.
    
    Architecture:
    - GCN Layer 1: feature_dim -> hidden_dim (ReLU + Dropout)
    - GCN Layer 2: hidden_dim -> num_classes
    """

    def __init__(self, num_node_features, hidden_dim, num_classes, dropout=0.5):
        super(GCN, self).__init__()
        self.conv1 = pgl.layers.GCNConv(num_node_features, hidden_dim)
        self.conv2 = pgl.layers.GCNConv(hidden_dim, num_classes)
        self.dropout = dropout

    def forward(self, graph, feat):
        """
        Forward pass through GCN.
        
        Args:
            graph: pgl.Graph object
            feat: Node feature matrix [num_nodes, feature_dim]
            
        Returns:
            Logits for each node [num_nodes, num_classes]
        """
        # First GCN layer with ReLU and dropout
        x = self.conv1(graph, feat)
        x = paddle.nn.functional.relu(x)
        x = paddle.nn.functional.dropout(x, p=self.dropout, training=False)
        
        # Second GCN layer (no activation for classification logits)
        x = self.conv2(graph, x)
        
        return x


# Entry point: GeaFlow will instantiate this class
transform_class = PaddleGCNTransform
