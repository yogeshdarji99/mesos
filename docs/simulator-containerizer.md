---
layout: documentation
---

# Simulator Containerizer

## Overview

The Simulator Containerizer (SC) is a containerizer that can be used in conjunction with mesos-local to exercise frameworks at a large scale without launching a large cluster. The SC launches a "fake" executor instead of the one specified by the tasks, in order to avoid using actual resources. The SC provides normal tasks updates as well as configurable task options via a configuration JSON file.

## Usage

### With a standalone slave

To launch a slave with SC, enable it via the `--containerizers` flag:

    --containerizers=simulator

Or, use the `MESOS_CONTAINERIZERS` environment variable:

    export MESOS_CONTAINERIZERS=simulator

Optionally, specify a path to the simulator config file (described in more detail below):

    --simulator_config=/Path/to/simulator/config/json

### With mesos-local (recommended)

`mesos-local` is a utility included with mesos to launch an in-memory cluster within a single process.  More often than not, you will want to use `mesos-local` with SC to simulate a large number of slaves.

To do so, first set the environment variable to enable SC:

    export MESOS_CONTAINERIZERS=simulator
    # If you want to use a custom configuration
	  export MESOS_SIMULATOR_CONFIG=/Path/to/simulator/config/json

If `mesos-local` is already available in your `PATH`, you can launch it using:

	mesos-local --num_slaves=x

If you built Mesos from source, `mesos-local` can be launched from the build directory using:

	./bin/mesos-local.sh --num_slaves=x

`--num_slaves` specifies the number of slaves you want to simulate.

After launching `mesos-local`, you may connect frameworks to the local cluster and launch tasks as you normally would.  Your frameworks will then receive status updates as specified by the configuration file described later in this document.

### Suggested Options

Some suggested environmental variables to set include:

```
export MESOS_REGISTRY=in_memory
export MESOS_REGISTRY_FETCH_TIMEOUT=1hrs
export MESOS_REGISTRY_STORE_TIMEOUT=1hrs
export MESOS_MAX_SLAVE_PING_TIMEOUT=30000
export MESOS_SLAVE_PING_TIMEOUT=15mins
export MESOS_EXECUTOR_REGISTRATION_TIMEOUT=1hrs
export MESOS_OFFER_TIMEOUT=1hrs
export MESOS_ZK_SESSION_TIMEOUT=1hrs
```

These help prevent larger simulated clusters from failing due to timeouts that occur as a result of running a large number of libprocess processes on the same event loop.

## Configuration JSON format

The JSON schema for the configuration file is given below, along with an example with explanations.

```json
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "Simulator Config",
    "description": "A simulator configuration file",
    "type": "object",
    "properties": {
        "task_specs": {
            "description": "The array of task specifications to apply to tasks run with simulator containerizer",
            "type": "array",
            "items": {
                "type": "object",
                "description": "task specification to apply to task that match the pattern",
                "properties": {
                    "pattern": {
                        "type": "string"
                    },
                    "failure_rate": {
                        "type": "number"
                    },
                    "length_mean": {
                        "type": "number"
                    },
                    "length_std": {
                        "type": "number"
                    }
                },
                "minItems": 1,
                "uniqueItems": true
            }
        }
    }
}
```

#### JSON file example
```json
{
    "task_specs": [
        {
            "pattern": "SOME_PREFIX.*",
            "failure_rate": 0.3,
            "length_mean": 2.9827,
            "length_std": 1.231
        },
        {
            "pattern": ".*",
            "failure_rate": 0.1,
            "length_mean": 10.23052,
            "length_std": 5.25329
        }
    ]
}
```

`pattern` is a regex pattern used to match task IDs.

`length_mean` and `length_std` are in seconds.

To explain, the first spec says that: all tasks whose IDs start with `SOME_PREFIX` have a 30% chance of failing.  On average, they should run 2.9827 seconds.  The standard deviation for their run time is 1.231 seconds.  Note that the task lengths follow a normal distribution.

The second spec is a catch-all (`.*`) that will match all task IDs.  Note that the ordering of the specs is important: if you had put the catch-all spec as the first spec, no other specs will have an effect at all.
