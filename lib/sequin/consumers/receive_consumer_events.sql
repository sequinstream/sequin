with deliverable_events as (
  select
    ce.id
  from
    sequin_streams.consumer_events ce
  where
    ce.consumer_id = :consumer_id
    and (ce.not_visible_until is null or ce.not_visible_until <= :now)
    -- Only select the first event for each group_id
    and not exists (
      select 1
      from sequin_streams.consumer_events earlier
      where earlier.consumer_id = ce.consumer_id
        and ((ce.group_id is not null and earlier.group_id = ce.group_id) or (ce.group_id is null and earlier.record_pks = ce.record_pks))
        and earlier.table_oid = ce.table_oid
        and earlier.seq < ce.seq
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