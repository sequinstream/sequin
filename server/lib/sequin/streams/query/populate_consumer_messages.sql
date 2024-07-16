WITH current_outstanding_count AS (
  SELECT
    count(*) AS count
  FROM
    sequin_streams.consumer_messages
  WHERE
    consumer_id = :consumer_id
),
max_new_consumer_messages AS (
  SELECT
    :max_consumer_message_count -(
      SELECT
        count
      FROM
        current_outstanding_count) AS max_new_consumer_messages
),
consumer_state AS (
  SELECT
    *
  FROM
    sequin_streams.consumer_states
  WHERE
    consumer_id = :consumer_id
),
new_messages AS (
  SELECT
    *
  FROM
    sequin_streams.messages m
  WHERE
    m.stream_id = :stream_id
    AND m.seq >(
      SELECT
        message_seq_cursor
      FROM
        consumer_state)
    ORDER BY
      m.seq ASC
    LIMIT (
      SELECT
        max_new_consumer_messages
      FROM
        max_new_consumer_messages)
),
new_consumer_messages AS (
INSERT INTO sequin_streams.consumer_messages AS cm(consumer_id, message_key, message_seq, message_stream_id, state, inserted_at, updated_at)
  SELECT
    :consumer_id,
    m.key,
    m.seq,
    m.stream_id,
    'available',
    :now,
    :now
  FROM
    new_messages m
  ON CONFLICT (consumer_id,
    message_key)
    DO UPDATE SET
      state =(
        CASE WHEN cm.state = 'delivered'::sequin_streams.consumer_message_state THEN
          'pending_redelivery'::sequin_streams.consumer_message_state
        ELSE
          cm.state
        END),
      message_seq = excluded.message_seq,
      updated_at = excluded.updated_at)
UPDATE
  sequin_streams.consumer_states
SET
  message_seq_cursor =(
    SELECT
      coalesce(max(seq),(
          SELECT
            message_seq_cursor
          FROM consumer_state))
    FROM
      new_messages),
  count_pulled_into_outstanding = count_pulled_into_outstanding +(
    SELECT
      count(*)
    FROM
      new_messages)
WHERE
  consumer_id = :consumer_id
