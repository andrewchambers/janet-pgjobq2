(import jdn)
(import sh)
(import posix-spawn)
(import redis)
(import pq)
(import shlex)
(import tmppg)
(import ../pgjobq2 :as pgjobq)

(defn run-tests
  []
  (with [tmp-pg (tmppg/tmppg)]

    (def pg-conn (pq/connect (tmp-pg :connect-string)))

    # Setup the job tables
    (pq/exec pg-conn "BEGIN TRANSACTION;")
    (each stmt (string/split "\n\n" pgjobq/job-schema)
      (pq/exec pg-conn stmt))
    (pq/exec pg-conn "COMMIT;")

    # basic sanity.
    (do
      (def q "test-q")
      (assert (nil? (pgjobq/next-job pg-conn q)))
      (assert (pgjobq/try-enqueue-job pg-conn q @{"some" "job"}))
      (assert (pgjobq/try-enqueue-job pg-conn q @{"some" "other-job"}))
      (assert (nil? (pgjobq/try-enqueue-job pg-conn q @{"some" "too many jobs"} 2)))
      (assert (= (pgjobq/count-pending-jobs pg-conn q) 2))
      (assert (deep= (pgjobq/next-job pg-conn q)
                     @{:jobid (int/s64 1) :position (int/s64 1) :q q :data @{"some" "job"}}))
      (pgjobq/publish-job-result pg-conn (int/s64 1) @{"status" "done"})
      (pgjobq/reschedule-job pg-conn 2)
      (assert (deep= (pgjobq/next-job pg-conn q)
                     @{:jobid (int/s64 2) :position (int/s64 3) :q q :data @{"some" "other-job"}})))
    
    ))

(run-tests)
