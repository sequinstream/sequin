# def wrapper(record, changes):
#     jrecord = json.decode(record)
#     jchanges = json.decode(changes)

#     return transform(jrecord, jchanges)


def transform(record, changes):
    jrecord = json.decode(record)
    jchanges = json.decode(changes)

    filtered_record = {k: v for (k, v) in jrecord.items() if not k.startswith("secret")}
    filtered_changes = {
        k: v for (k, v) in jchanges.items() if not k.startswith("secret")
    }

    filtered_record["timestamp_micro"] = ts_unix_micro(filtered_record["timestamp"])

    return {
        "record": filtered_record,
        "changes": filtered_changes,
        "timestamp": timestamp(),
        "number": 12345,
    }
