WITH pending_messages AS (
  SELECT
    *
  FROM
    streams.consumer_messages
  WHERE
    consumer_id = :consumer_id
  ORDER BY
    message_seq ASC
  LIMIT :max_ack_pending
),
deliverable_messages AS (
  SELECT
    *
  FROM
    pending_messages
  WHERE
    state = 'available'
    OR (state IN ('delivered', 'pending_redelivery')
      AND not_visible_until <= :now)
  ORDER BY
    message_seq ASC
  LIMIT :batch_size
),
updated_messages AS (
  UPDATE
    streams.consumer_messages cm
  SET
    state = 'delivered',
    not_visible_until = :not_visible_until,
    deliver_count = cm.deliver_count + 1,
    last_delivered_at = :now
  FROM
    deliverable_messages dm
  WHERE
    cm.id = dm.id
  RETURNING
    cm.id AS ack_id,
    cm.message_key,
    cm.message_stream_id
)
SELECT
  um.ack_id,
  m.*
FROM
  updated_messages um
  JOIN streams.messages m ON m.key = um.message_key
    AND m.stream_id = um.message_stream_id
  ORDER BY
    m.seq ASC
