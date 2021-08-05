(import pq)

(def job-schema `
  create table jobq(jobid bigserial primary key, position bigserial, q TEXT, data TEXT, result TEXT, completedat bigint);

  create index jobqqindex on jobq(q);

  create index jobqpositionindex on jobq(q);

  create index jobqcompletedatindex on jobq(completedat);
`)

(defn count-pending-jobs
  [pg-conn q]
  (pq/val pg-conn "select count(*)::integer from jobq where q = $1 and completedat is null;" q))

(defn enqueue-job
  [pg-conn q job-data]
  (def jobid
    (pq/val pg-conn "insert into jobq(q, data) values($1, $2) returning jobid;" q (string/format "%j" job-data)))
  (pq/exec pg-conn (string "notify \"jobq-" q "-new\";"))
  jobid)

(defn try-enqueue-job
  [pg-conn q job-data limit]
  (if (< (count-pending-jobs pg-conn q) limit)
    (enqueue-job pg-conn q job-data)
    nil))

(defn reschedule-job
  [pg-conn jobid]
  (pq/exec pg-conn
           "update jobq set position = nextval('jobq_position_seq') where jobid = $1;"
           jobid))

(defn publish-job-result
  [pg-conn jobid result]
  (def q (pq/val pg-conn "select q from jobq where jobid = $1;" jobid))
  (unless q
    (error "no such job"))
  (pq/exec
    pg-conn
    "update jobq set result = $1, completedat = extract(epoch from now()) where jobid = $2 and completedat is null;"
    (string/format "%j" result) jobid)
  (pq/exec pg-conn (string "notify \"jobq-" q "-complete\", '" jobid "';"))
  nil)

(defn query-job
  [pg-conn jobid]
  (when-let [j (pq/row pg-conn "select jobid, data, result from jobq where jobid = $1;" jobid)]
    (put j :data (parse (j :data)))
    (when (j :result)
      (put j :result (parse (j :result))))
    j))

(defn query-job-result
  [pg-conn jobid]
  (when-let [r (pq/val pg-conn "select result from jobq where jobid = $1;" jobid)]
    (parse r)))

(defn next-job
  [pg-conn q]
  (def j (pq/row pg-conn "select * from jobq where (q = $1 and completedat is null) order by position asc limit 1;" q))
  (when j
    (put j :data (parse (j :data))))
  j)

(defn run-worker
  [pg-conn q run-job &keys {:fallback-poll-timer fallback-poll-timer}]
  (default fallback-poll-timer 600)
  (pq/exec pg-conn (string "listen \"jobq-" q "-new\";"))
  (while true
    (def j
      (if-let [j (next-job pg-conn q)]
        j
        (do
          (pq/wait-for-and-discard-notifications pg-conn fallback-poll-timer)
          (next-job pg-conn q))))
    (when (not (nil? j))
      (match (run-job pg-conn (j :data))
        [:job-complete result]
        (publish-job-result pg-conn (j :jobid) result)
        :reschedule
        (reschedule-job pg-conn (j :jobid))
        v
        (errorf "job worker returned an unexpected result %p" v)))))

(defn wait-for-job-completion
  [pg-conn jobid timeout]
  (if-let [result (query-job-result pg-conn jobid)]
    result
    (do
      (def q (pq/val pg-conn "select q from jobq where jobid = $1;" jobid))
      (unless q
        (error "no such job"))
      (def jobid-str (string jobid))
      (def channel (string "jobq-" q "-complete"))
      (pq/exec pg-conn (string "listen \"" channel "\";"))
      (defer (do
               (pq/exec pg-conn (string "unlisten \"" channel "\";"))
               (pq/discard-notifications pg-conn))
        (var result (query-job-result pg-conn jobid))
        (while (nil? result)
          (def notifications (pq/wait-for-notifications pg-conn timeout))
          (when (empty? notifications)
            (break))
          (each n notifications
            (when (and (= channel (n :name))
                       (= jobid-str (n :extra)))
              (set result (query-job-result pg-conn jobid))
              (unless result
                (error "job is missing result")))))
        result))))

# Repl test helpers.
# (import ./pgjobq2 :as pgjobq)
# (def pg-conn (pq/connect "postgresql://localhost?dbname=postgres"))
# (pq/exec pg-conn "BEGIN TRANSACTION;")
#   (each stmt (string/split "\n\n" pgjobq/job-schema)
#     (pq/exec pg-conn stmt))
# (pq/exec pg-conn "COMMIT;")
# (pgjobq/try-enqueue-job pg-conn "testq" @{"hello" "world"})

