---
layout: site
title: Use SystemDS with Docker
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->


[Docker](https://docs.docker.com/get-docker/) enables you to separate applications from
your infrastructure. This provides a way to manage the instrafrastructure the same way
you do with the software.

With Docker, enabling GPU support would be much easier on linux. Since only the NVIDIA
GPU drivers are required on the host machine (NVIDIA CUDA toolkit is not required).

## SystemDS Docker requirements

1. Install [Docker](https://docs.docker.com/get-docker/) specific to your machine
2. Install [CUDA enabled docker](https://github.com/NVIDIA/nvidia-docker) image with
   [installation instructions](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html#installation-guide)
    - You might want to register NVIDIA runtime. If the docker version (`docker -v`) is earlier than
      19.03 `nvidia-docker2` package registers runtime. But, version 19.03 including and higher
      use [toolkit guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/user-guide.html)
      to register the runtime.


Note: If you would like to manage docker as a non-root user, refer to
[linux-postinstall](https://docs.docker.com/engine/install/linux-postinstall/)

## Download SystemDS Docker image

The official SystemDS docker images are located at [apache/systemds](https://hub.docker.com/r/apache/systemds)
Docker Hub repository. Image releases are tagged based on the release channel:

| Tag | Description |
| --- | --- |
| `latest` | The latest release of SystemDS CPU image. Default version. |
| `version` | Specify the exact version |
| `dev` | Builds for SystemDS `master` branch. Used by SystemDS developers |

For GPU or additional functionality, use tags to the base variants:

| Tag Variants | Description |
| --- | --- |
| `tag-gpu` | The specified tag release with GPU support. |

Usage examples:

```sh
docker pull apache/systemds         # Latest stable release with CPU
docker pull apache/systemds:dev-gpu # Dev release with GPU
```

### Start the Docker container


Options:

`-it` - interactive
`--rm` - cleanup
`-p` - port forwarding

For comprehensive guide, refer [`docker run`](https://docs.docker.com/engine/reference/run/)

```sh

docker run [-it] [--rm] [-p hostPort:containerPort] apache/systemds[:tag} [command]
```

Examples

To verify the SystemDS installation,

```sh
docker run -it --rm apache/systemds \
  /bin/bash -c "echo 'print("This is SystemDS!")' > hello.dml && systemds hello.dml"
```

The output is `"This is SystemDS!"` after successful installation.
For SystemDS usage instructions, see [standalone instructions](./run).

To run a DML program developed on the host machine, mount the host directory and change the
working directory with [`-v` flag](https://docs.docker.com/engine/reference/run/#volume-shared-filesystems)
and [`-w` flags](https://docs.docker.com/engine/reference/run/#workdir).


```sh
touch ./script.py

cat << EOF >> ./script.py
# Import SystemDSContext
from systemds.context import SystemDSContext
# Create a context and if necessary (no SystemDS py4j instance running)
# it starts a subprocess which does the execution in SystemDS
with SystemDSContext() as sds:
    # Full generates a matrix completely filled with one number.
    # Generate a 5x10 matrix filled with 4.2
    m = sds.full((5, 10), 4.20)
    # multiply with scalar. Nothing is executed yet!
    m_res = m * 3.1
    # Do the calculation in SystemDS by calling compute().
    # The returned value is an numpy array that can be directly printed.
    print(m_res.compute())
# context will automatically be closed and process stopped
EOF

docker run -it --rm -v $PWD:/tmp -w /tmp apache/systemds python ./script.py
```

## Running with GPU

Check for the GPU devices:

```sh
lspci | grep -i nvidia
```

And verify `nvidia-docker` installation:

```sh
docker run --gpus all --rm nvidia/cuda nvidia-smi

# nvidia-docker v2
# docker run --runtime=nvidia nvidia/cuda nvidia-smi
```
