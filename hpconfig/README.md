# hpconfig
`hpconfig` provides class-based specifications for different HPC hardware and
 environments. 
 
Hardware defintions inherit from the `CPU_NODE` and `GPU_NODE
`classes defined in `hpconfig.utils.classes`. These are used in cluster
descriptions of HPC configurations, referred to as 'specs`. 
`hpconfig` contains a number of different pre-defined specs, such as the
Pawsey supercomputing facility , and some planned infrastructures, such as
the Square Kilometre Science Data Processors. 
  
 ## Defining a `hpconfig` spec
 An example class may be constructed as follows: 
 
 ```python
from hpconfig.utils.constants import SI
from hpconfig.utils.classes import CPU_NODE, GPU_NODE


class MyStartingSpec:
    IntelXeonSpec = CPU_NODE(
        name="IntelXeonSpec",
        cores=4,
        flops_per_cycle=8,
        ncycles=2.0 * SI.giga,
        bandwidth=1877 * SI.mega
    )  # ~240 giga flops

# ...
# ...
``` 

A spec consists of one or more hardware device (CPU or GPU) as class
attributes. These are then collated in the `__init__()` function in the
 architecture definition:
 
```python
def __init__(self):

    self.name = 'MyStartingCluster'
    memory_per_cpu_node = 64
    memory_per_gpu_node = 32
    self.architecture = {
        'cpu': {
            self.XeonIvyBridge: 50,
            self.XeonSandyBridge: 100,
        },
        'gpu': {
            self.NvidiaKepler: 64
        }
    }
``` 
     

