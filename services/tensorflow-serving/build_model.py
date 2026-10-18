import logging
import os
import sys

import numpy as np
import tensorflow as tf


LOG = logging.getLogger("model_builder")


class ExampleModel(tf.Module):
    def __init__(self, model):
        super().__init__()
        self.model = model

    @tf.function(
        input_signature=[
            tf.TensorSpec(shape=[None, 1], dtype=tf.float32, name="features")
        ]
    )
    def predict(self, features):
        predictions = self.model(features, training=False)
        tf.print(
            "ExampleModel.predict invoked; features=",
            features,
            " predictions=",
            predictions,
            summarize=8,
        )
        return {"predictions": predictions}


def train_model():
    tf.keras.utils.set_random_seed(2026)
    features = np.linspace(-4.0, 4.0, 1201, dtype=np.float32).reshape(-1, 1)
    targets = (np.sin(features) + 0.3 * features).astype(np.float32)

    normalization = tf.keras.layers.Normalization()
    normalization.adapt(features)
    model = tf.keras.Sequential(
        [
            tf.keras.Input(shape=(1,)),
            normalization,
            tf.keras.layers.Dense(32, activation="tanh"),
            tf.keras.layers.Dense(32, activation="tanh"),
            tf.keras.layers.Dense(1),
        ],
        name="nonlinear_feature_regressor",
    )
    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.01), loss="mse")
    LOG.info("Training a two-layer nonlinear regression model on %d samples", len(features))
    history = model.fit(
        features,
        targets,
        epochs=80,
        batch_size=32,
        validation_split=0.2,
        callbacks=[
            tf.keras.callbacks.EarlyStopping(
                monitor="val_loss", patience=8, restore_best_weights=True
            )
        ],
        verbose=0,
    )
    LOG.info(
        "Training finished: epochs=%d final_loss=%.6f final_val_loss=%.6f",
        len(history.history["loss"]),
        history.history["loss"][-1],
        history.history["val_loss"][-1],
    )
    return model


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    export_path = os.path.abspath(sys.argv[1])
    model = train_model()
    serving_model = ExampleModel(model)
    tf.saved_model.save(
        serving_model,
        export_path,
        signatures={"serving_default": serving_model.predict},
    )
    LOG.info("Saved TensorFlow Serving model to %s", export_path)
