-- Fill placeholders before running in Supabase SQL editor.
-- Requires extensions: pg_cron and pg_net.

create extension if not exists pg_cron with schema extensions;
create extension if not exists pg_net with schema extensions;

select cron.unschedule('monitor-heartbeat')
where exists (
  select 1
  from cron.job
  where jobname = 'monitor-heartbeat'
);

select cron.schedule(
  'monitor-heartbeat',
  '*/5 * * * *',
  $$
  select net.http_post(
    url := 'https://PROJECT_REF.supabase.co/functions/v1/monitor-heartbeat',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer SUPABASE_ANON_KEY',
      'x-monitor-secret', 'MONITOR_SECRET'
    ),
    body := '{}'::jsonb,
    timeout_milliseconds := 10000
  );
  $$
);

-- Keep pg_cron logs small.
select cron.unschedule('monitor-cron-log-cleanup')
where exists (
  select 1
  from cron.job
  where jobname = 'monitor-cron-log-cleanup'
);

select cron.schedule(
  'monitor-cron-log-cleanup',
  '0 3 * * *',
  $$
  delete from cron.job_run_details
  where start_time < now() - interval '7 days';
  $$
);
