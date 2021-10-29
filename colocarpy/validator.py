"""
Copyright 2018 The Johns Hopkins University Applied Physics Laboratory.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from abc import ABC, abstractmethod
from typing import Tuple 

class PointValidator(ABC):
    
    @classmethod
    @abstractmethod
    def validate_point(cls, coordinate, resolution):
        raise NotImplementedError()

class Minnie65Validator(PointValidator):
    x_start, x_stop = 13824, 226816
    y_start, y_stop = 13824, 194048
    z_start, z_stop = 14816, 27904
    
    @classmethod
    def validate_point(cls, coordinate:Tuple[int]):
        return ((cls.x_start <= coordinate[0] <= cls.x_stop) and 
            (cls.y_start <= coordinate[1] <= cls.y_stop) and 
            (cls.z_start <= coordinate[2] <= cls.z_stop))