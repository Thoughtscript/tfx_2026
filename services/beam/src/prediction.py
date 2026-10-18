import json


def prediction_payload(feature):
    return {
        "signature_name": "serving_default",
        "instances": [{"features": [float(feature)]}],
    }


def parse_prediction_response(response_body):
    predictions = json.loads(response_body)["predictions"]
    if len(predictions) != 1:
        raise ValueError(f"Expected one prediction, received {len(predictions)}")
    prediction = predictions[0]
    if isinstance(prediction, list):
        if len(prediction) != 1:
            raise ValueError(f"Expected one scalar prediction, received {len(prediction)}")
        prediction = prediction[0]
    return float(prediction)
