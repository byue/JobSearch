-- 1) Run state / publish lifecycle
CREATE TABLE IF NOT EXISTS publish_runs (
  run_id               TEXT PRIMARY KEY,
  version_ts           TIMESTAMPTZ NOT NULL,
  status               TEXT NOT NULL CHECK (status IN (
                           'in_progress', 'failed', 'succeeded', 'skipped'
                         )),

  -- DB side
  db_ready             BOOLEAN NOT NULL DEFAULT FALSE,
  db_published_at      TIMESTAMPTZ,
  db_error_message     TEXT,

  -- ES side
  es_ready             BOOLEAN NOT NULL DEFAULT FALSE,
  es_published_at      TIMESTAMPTZ,
  es_error_message     TEXT,

  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS publish_runs_status_idx ON publish_runs (status);
CREATE INDEX IF NOT EXISTS publish_runs_version_idx ON publish_runs (version_ts DESC);


-- 2) Namespace-based publication pointer
CREATE TABLE IF NOT EXISTS publication_pointers (
  namespace            TEXT PRIMARY KEY,              -- e.g. 'jobs_catalog'
  run_id               TEXT NOT NULL REFERENCES publish_runs(run_id),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- bootstrap FK-safe seed
INSERT INTO publish_runs (run_id, version_ts, status, db_ready, es_ready)
VALUES ('bootstrap', now(), 'skipped', TRUE, TRUE)
ON CONFLICT (run_id) DO NOTHING;

INSERT INTO publication_pointers (namespace, run_id)
VALUES ('jobs_catalog', 'bootstrap')
ON CONFLICT (namespace) DO NOTHING;


-- 3) Versioned jobs
CREATE TABLE IF NOT EXISTS jobs (
  run_id               TEXT NOT NULL REFERENCES publish_runs(run_id),
  version_ts           TIMESTAMPTZ NOT NULL,
  company              TEXT NOT NULL,
  external_job_id      TEXT NOT NULL,
  title                TEXT,
  job_type             TEXT,
  details_url          TEXT,
  apply_url            TEXT,
  city                 TEXT,
  state                TEXT,
  country              TEXT,
  skills               JSONB NOT NULL DEFAULT '[]'::jsonb,
  job_description_embedding JSONB NOT NULL DEFAULT '[]'::jsonb,
  posted_ts            TIMESTAMPTZ,
  is_missing_details   BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, company, external_job_id)
);

CREATE INDEX IF NOT EXISTS jobs_run_company_idx ON jobs (run_id, company);
CREATE INDEX IF NOT EXISTS jobs_run_posted_idx ON jobs (run_id, posted_ts DESC);
CREATE INDEX IF NOT EXISTS jobs_version_idx ON jobs (version_ts);

-- 4) Versioned job details
CREATE TABLE IF NOT EXISTS job_details (
  run_id                     TEXT NOT NULL,
  version_ts                 TIMESTAMPTZ NOT NULL,
  company                    TEXT NOT NULL,
  external_job_id            TEXT NOT NULL,
  job_description_path       TEXT,
  updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, company, external_job_id),
  FOREIGN KEY (run_id, company, external_job_id)
    REFERENCES jobs(run_id, company, external_job_id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS job_details_run_company_idx ON job_details (run_id, company);
CREATE INDEX IF NOT EXISTS job_details_version_idx ON job_details (version_ts);

-- 5) Versioned companies
CREATE TABLE IF NOT EXISTS companies (
  run_id               TEXT NOT NULL REFERENCES publish_runs(run_id),
  version_ts           TIMESTAMPTZ NOT NULL,
  company              TEXT NOT NULL,
  display_name         TEXT NOT NULL,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, company)
);

CREATE INDEX IF NOT EXISTS companies_version_idx ON companies (version_ts);
