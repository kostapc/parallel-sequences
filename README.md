# parallel-sequences
parallel processing sequential tasks defined by some key

## principe
For each key new thread spawning and all tasks for this key will be processed by one thread sequentially. When tasks is over thread will be released and returned to Executor.
