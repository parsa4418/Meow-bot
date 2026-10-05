create table if not exists tg_session (
  id text primary key,
  data text not null
);
alter table tg_session enable row level security; -- فقط با service_role key قابل دسترسی
