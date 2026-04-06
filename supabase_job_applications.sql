-- ============================================================
-- job_applications table
-- Run this in the Supabase SQL editor (project > SQL Editor)
-- ============================================================

-- 1. Create the table
CREATE TABLE IF NOT EXISTS public.job_applications (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    job_id      TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'applied'
                            CHECK (status IN ('applied','interviewing','offered','rejected')),
    notes       TEXT,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One row per user per job
    UNIQUE (user_id, job_id)
);

-- 2. Keep updated_at fresh automatically
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_job_applications_updated_at
BEFORE UPDATE ON public.job_applications
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 3. Row Level Security — users can only see and modify their own rows
ALTER TABLE public.job_applications ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view own applications"
    ON public.job_applications FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "Users can insert own applications"
    ON public.job_applications FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own applications"
    ON public.job_applications FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "Users can delete own applications"
    ON public.job_applications FOR DELETE
    USING (auth.uid() = user_id);

-- 4. Index for fast per-user lookups
CREATE INDEX IF NOT EXISTS idx_job_applications_user_id
    ON public.job_applications (user_id);
