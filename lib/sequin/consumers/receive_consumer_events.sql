with deliverable_events as (
  select
    ce.id,
    ce.commit_lsn,
    ce.record_pks,
    ce.table_oid
  from
    sequin_streams.consumer_events ce
  where
    ce.consumer_id = :consumer_id
    and (ce.not_visible_until is null or ce.not_visible_until <= :now)
    and not exists (
      select 1
      from sequin_streams.consumer_events outstanding
      where outstanding.consumer_id = :consumer_id
        and outstanding.not_visible_until > :now
        and outstanding.record_pks = ce.record_pks
        and outstanding.table_oid = ce.table_oid
    )
  order by
    ce.id asc
  limit :batch_size
  for update skip locked
)
update
  sequin_streams.consumer_events ce
set
  not_visible_until = :not_visible_until,
  deliver_count = ce.deliver_count + 1,
  last_delivered_at = :now,
  updated_at = :now,
  state = 'delivered'
where
  ce.id in (select de.id from deliverable_events de)
returning
  ce.*