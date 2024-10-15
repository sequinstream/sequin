with deliverable_records as (
  select
    cr.id,
    cr.commit_lsn,
    cr.group_id,
    cr.table_oid
  from
    sequin_streams.consumer_records cr
  where
    cr.consumer_id = :consumer_id
    and (cr.not_visible_until is null
      or cr.not_visible_until <= :now)
    and state != 'acked'
    and not exists (
      select
        1
      from
        sequin_streams.consumer_records outstanding
      where
        outstanding.consumer_id = :consumer_id
        and outstanding.not_visible_until > :now
        and outstanding.group_id = cr.group_id
        and outstanding.table_oid = cr.table_oid)
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
