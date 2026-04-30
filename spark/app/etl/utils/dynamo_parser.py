import json


def parse_dynamodb_value(typed_value):
    """
    Recursively unwrap DynamoDB typed JSON to native Python values.
    
    Handles: S (string), N (number), L (list), M (map), 
             BOOL (boolean), NULL, SS/NS (sets)
    """
    if typed_value is None:
        return None

    if "S" in typed_value:
        return typed_value["S"]
    elif "N" in typed_value:
        num_str = typed_value["N"]
        return float(num_str) if "." in num_str else int(num_str)
    elif "L" in typed_value:
        return [parse_dynamodb_value(item) for item in typed_value["L"]]
    elif "M" in typed_value:
        return {k: parse_dynamodb_value(v) for k, v in typed_value["M"].items()}
    elif "BOOL" in typed_value:
        return typed_value["BOOL"]
    elif "NULL" in typed_value:
        return None
    elif "SS" in typed_value:
        return typed_value["SS"]
    elif "NS" in typed_value:
        return [float(n) if "." in n else int(n) for n in typed_value["NS"]]

    return None


def parse_kinesis_payload(payload_str):
    """
    Parse a Kinesis CDC record from DynamoDB Streams.
    
    Returns:
        dict with keys: table_name, event_name, record (parsed NewImage)
    """
    payload = json.loads(payload_str)

    table_name = payload.get("tableName")
    event_name = payload.get("eventName")  # INSERT, MODIFY, REMOVE

    new_image = payload.get("dynamodb", {}).get("NewImage", {})
    parsed_record = {
        key: parse_dynamodb_value(typed_val)
        for key, typed_val in new_image.items()
    }

    return {
        "table_name": table_name,
        "event_name": event_name,
        "record": parsed_record,
    }