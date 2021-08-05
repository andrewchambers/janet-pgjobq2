# janet-pgjobq2

A backend job queue with some useful properties:

- Simple to operate, only depends on postgres, which you probably use already.

- Guaranteed processing of jobs in a single queue in fifo order. Allowing a
  simplified mental model, jobs queued later, will be executed after earlier jobs finish.

- Jobs are added during a database transaction, meaning all jobs are rolled back
  on error, and many jobs can be atomically added in a single transaction.

- Optional back pressure so you can return errors to users when the queue is full,
  with transactional rollback.

## Quick example

In a web example, enqueue a job as part of a database transaction:
```
(in-transaction... 
  (def job @{"your" "job"})
  (def jobid (pgjobq/try-enqueue-job pg-conn "your-job-queue" job Q-LIMIT))
  (unless jobid (error "job queue over loaded")))
```

In another process...

```
(run-worker
  pg-conn "your-job-queue"
  (fn run-job [pg-conn job] [:job-complete :ok]))
```

## Usage tips

- DO NOT run multiple workers per queue.
- Jobs and results are stored in the table ```jobq```, the required schema tables
  in postgres can be viewed in pgjobq.janet.
- Jobs are encoded as jdn, only types that support jdn will work.
- Run the job worker in a process supervisor with restarts, it deliberately does NOT catch errors,
  this is so bugs like fd leaks can be recovered from without admin intervention.
- For concurrent queue processing, when creating jobs, do queue sharding
  and launch one worker per shard.
- It's a good idea to add your own locking


