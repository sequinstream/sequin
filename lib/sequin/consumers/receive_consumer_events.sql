with outstanding_count as (
  select
    COUNT(*) as count
  from
    sequin_streams.consumer_events
  where
    consumer_id = :consumer_id
    and not_visible_until > :now
),
deliverable_events as (
  select
    ce.id,
    ce.commit_lsn,
    ce.record_pks,
    ce.table_oid
  from
    sequin_streams.consumer_events ce
    left join (
      select
        record_pks,
        table_oid
      from
        sequin_streams.consumer_events
      where
        consumer_id = :consumer_id
        and not_visible_until > :now) as outstanding on ce.record_pks = outstanding.record_pks
    and ce.table_oid = outstanding.table_oid
  where
    ce.consumer_id = :consumer_id
    and (ce.not_visible_until is null
      or ce.not_visible_until <= :now)
    and outstanding.record_pks is null
  order by
    ce.commit_lsn asc
  limit GREATEST(:max_ack_pending -(
      select
        count
      from outstanding_count), 0))
update
  sequin_streams.consumer_events ce
set
  not_visible_until = :not_visible_until,
  deliver_count = ce.deliver_count + 1,
  last_delivered_at = :now,
  updated_at = :now
where
  ce.id in (
    select
      de.id
    from
      deliverable_events de
      join sequin_streams.consumer_events ce_inner on ce_inner.id = de.id
    order by
      ce_inner.commit_lsn asc
    limit :batch_size
    for update
      of ce_inner skip locked)
returning
  ce.*
