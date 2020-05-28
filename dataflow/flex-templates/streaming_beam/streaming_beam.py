#!/usr/bin/env python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An Apache Beam streaming pipeline example.

It reads JSON encoded messages from Pub/Sub, transforms the message data and
writes the results to BigQuery.
"""

import argparse
import json
import logging
import time

# Unused dependency used to replicate bug.
import matplotlib
import numpy as np

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window


def useless_numpy_function(x):
    return str(np.array(x))


def run(args, output_text):
    """Build and run the pipeline."""
    options = PipelineOptions(args, save_main_session=True)

    with beam.Pipeline(options=options) as pipeline:

        # Read the messages from PubSub and process them.
        _ = (
            pipeline
            | "Create tiny collection" >> beam.Create(["a", "b", "c"])
            | "Useless Numpy Function" >> beam.Map(useless_numpy_function)
            | "Write output" >> beam.io.Write(beam.io.WriteToText(output_text))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_text", help="Path to output location (should be in a bucket)"
    )

    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args, known_args.output_text)
