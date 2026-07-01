create extension if not exists pgcrypto with schema extensions;

create table if not exists public.server_heartbeat (
  id text primary key,
  updated_at timestamptz not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

alter table public.server_heartbeat enable row level security;

drop policy if exists "server heartbeat is readable" on public.server_heartbeat;
create policy "server heartbeat is readable"
  on public.server_heartbeat
  for select
  to authenticated
  using (true);

create table if not exists public.server_heartbeat_tokens (
  id text primary key,
  token_hash text not null,
  updated_at timestamptz not null default now()
);

alter table public.server_heartbeat_tokens enable row level security;

create table if not exists public.monitor_state (
  id text primary key,
  state text not null check (state in ('up', 'degraded', 'down')),
  reason text,
  reason_key text,
  metadata jsonb not null default '{}'::jsonb,
  notified_at timestamptz,
  updated_at timestamptz not null default now()
);

alter table public.monitor_state enable row level security;

create or replace function public.set_server_heartbeat_token(
  p_id text,
  p_token text
) returns void
language plpgsql
security definer
set search_path = public, extensions
as $$
begin
  if p_id is null or length(p_id) = 0 then
    raise exception 'p_id required';
  end if;

  if p_token is null or length(p_token) < 32 then
    raise exception 'p_token must be at least 32 characters';
  end if;

  insert into public.server_heartbeat_tokens (id, token_hash, updated_at)
  values (p_id, encode(digest(p_token, 'sha256'), 'hex'), now())
  on conflict (id) do update
    set token_hash = excluded.token_hash,
        updated_at = excluded.updated_at;
end;
$$;

revoke all on function public.set_server_heartbeat_token(text, text)
  from public, anon, authenticated;

create or replace function public.submit_server_heartbeat(
  p_id text,
  p_payload jsonb,
  p_token text
) returns void
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  expected_hash text;
  provided_hash text;
begin
  if p_id is null or length(p_id) = 0 then
    raise exception 'p_id required';
  end if;

  if p_payload is null then
    raise exception 'p_payload required';
  end if;

  select token_hash
    into expected_hash
    from public.server_heartbeat_tokens
   where id = p_id;

  if expected_hash is null then
    raise exception 'heartbeat token not configured for %', p_id;
  end if;

  provided_hash := encode(digest(coalesce(p_token, ''), 'sha256'), 'hex');

  if provided_hash <> expected_hash then
    raise exception 'invalid heartbeat token';
  end if;

  insert into public.server_heartbeat (id, updated_at, payload)
  values (
    p_id,
    now(),
    p_payload
  )
  on conflict (id) do update
    set updated_at = excluded.updated_at,
        payload = excluded.payload;
end;
$$;

grant execute on function public.submit_server_heartbeat(text, jsonb, text)
  to anon, authenticated;
