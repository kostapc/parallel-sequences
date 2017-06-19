# parallel-sequences
parallel processing sequential tasks defined by some key. Different keys processed parallel.

## principe
For each key new thread taked from pool and all tasks for this key will be processed by one thread sequentially. When tasks is over thread will be released and returned to Executor.
