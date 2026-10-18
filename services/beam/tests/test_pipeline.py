import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.prediction import parse_prediction_response, prediction_payload


class PredictionPayloadTests(unittest.TestCase):
    def test_builds_tensorflow_serving_request(self):
        self.assertEqual(
            prediction_payload(2),
            {
                "signature_name": "serving_default",
                "instances": [{"features": [2.0]}],
            },
        )

    def test_parses_single_prediction(self):
        self.assertEqual(
            parse_prediction_response(json.dumps({"predictions": [[5.0]]})),
            5.0,
        )

    def test_rejects_unexpected_prediction_count(self):
        with self.assertRaisesRegex(ValueError, "Expected one prediction"):
            parse_prediction_response(json.dumps({"predictions": []}))


if __name__ == "__main__":
    unittest.main()
