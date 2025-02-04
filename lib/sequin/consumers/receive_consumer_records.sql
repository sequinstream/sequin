with deliverable_records as (
  select
    cr.id
  from
    sequin_streams.consumer_records cr
  where
    cr.consumer_id = :consumer_id
    and (cr.not_visible_until is null
      or cr.not_visible_until <= :now)
    -- Only select the first record for each group_id
    and not exists (
      select
        1
      from
        sequin_streams.consumer_records earlier
      where
        earlier.consumer_id = cr.consumer_id
        and earlier.group_id = cr.group_id
        and earlier.table_oid = cr.table_oid
        and (earlier.commit_lsn < cr.commit_lsn
          or (earlier.commit_lsn = cr.commit_lsn
            and earlier.commit_idx < cr.commit_idx)))
    order by
      cr.id asc
    limit :batch_size
    for update
      skip locked)
update
  sequin_streams.consumer_records cr
set
  not_visible_until = :not_visible_until,
  deliver_count = cr.deliver_count + 1,
  last_delivered_at = :now,
  updated_at = :now,
  state = 'delivered'
where
  cr.id in (
    select
      dr.id
    from
      deliverable_records dr)
returning
  cr.*
