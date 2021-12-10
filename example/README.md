# Example implementation of task worker

## How to run?

### Prerequisites

You must have docker installed. We need docker to run elasticmq container, 
where elasticmq is to emulate Amazon SQS on your local machine.

### Run ElasticMQ

    ./run_elasticmq.sh

### Run Worker process

    # You can run multiple such processes
    ./run_worker.sh

### Publish a task

    # The example task is to calculate fibonacci number
    # The script takes an integer argument $n as input,
    # so at the worker side, it'll calculate fibonacci($n)
    ./run_publisher.sh 7

