WITH deliverable_ids AS (
  SELECT ack_id
  FROM (
    SELECT ack_id, state, not_visible_until
    FROM sequin_streams.consumer_messages
    WHERE consumer_id = :consumer_id
    ORDER BY message_seq ASC
    LIMIT :max_ack_pending
  ) as max_ack_pending
  WHERE state = 'available'
     OR (state IN ('delivered', 'pending_redelivery') AND not_visible_until <= :now)
  LIMIT :batch_size
  FOR UPDATE SKIP LOCKED
),
updated AS (
  UPDATE sequin_streams.consumer_messages cm
  SET state = 'delivered',
      deliver_count = cm.deliver_count + 1,
      not_visible_until = :not_visible_until,
      last_delivered_at = :now,
      updated_at = :now
  FROM deliverable_ids d
  WHERE cm.ack_id = d.ack_id
  RETURNING cm.ack_id, cm.message_key, cm.message_seq
)
SELECT u.ack_id, m.*
FROM updated u
JOIN sequin_streams.messages m ON u.message_key = m.key AND u.message_seq = m.seq