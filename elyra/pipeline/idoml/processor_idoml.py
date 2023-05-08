#
# Copyright 2018-2022 Elyra Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import datetime
import os

from traitlets import CUnicode
from traitlets import List as ListTrait

from elyra.pipeline.pipeline import Pipeline
from elyra.pipeline.processor import RuntimePipelineProcessor
from elyra.pipeline.processor import RuntimePipelineProcessorResponse
from elyra.pipeline.runtime_type import RuntimeProcessorType
from elyra.util.path import get_absolute_path


class IdomlPipelineProcessor(RuntimePipelineProcessor):
    _type = RuntimeProcessorType.APACHE_AIRFLOW
    _name = "idoml"

    # Provide users with the ability to identify a writable directory in the
    # running container where the notebook | script is executed. The location
    # must exist and be known before the container is started.
    # Defaults to `/tmp`
    WCD = os.getenv("ELYRA_WRITABLE_CONTAINER_DIR", "/tmp").strip().rstrip("/")

    # This specifies the default airflow operators included with Elyra.  Any Airflow-based
    # custom connectors should create/extend the elyra configuration file to include
    # those fully-qualified operator/class names.
    available_airflow_operators = ListTrait(
        CUnicode(),
        [
            "airflow.operators.slack_operator.SlackAPIPostOperator",
            "airflow.operators.bash_operator.BashOperator",
            "airflow.operators.email_operator.EmailOperator",
            "airflow.operators.http_operator.SimpleHttpOperator",
            "airflow.contrib.operators.spark_sql_operator.SparkSqlOperator",
            "airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator",
        ],
        help="""List of available Apache Airflow operator names.

Operators available for use within Apache Airflow pipelines.  These operators must
be fully qualified (i.e., prefixed with their package names).
       """,
    ).tag(config=True)

    # Contains mappings from class to import statement for each available Airflow operator
    class_import_map = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.class_import_map:  # Only need to load once
            for package in self.available_airflow_operators:
                parts = package.rsplit(".", 1)
                self.class_import_map[parts[1]] = f"from {parts[0]} import {parts[1]}"
        self.log.debug(f"class_package_map = {self.class_import_map}")

    def process(self, pipeline: Pipeline) -> "IdomlPipelineProcessorResponse":
        """
        Submit the pipeline for execution on Apache Airflow.
        """
        print("IdomlPipelineProcessor.process")

    def export(
        self, pipeline: Pipeline, pipeline_export_format: str, pipeline_export_path: str, overwrite: bool
    ) -> str:
        """
        Export pipeline as Airflow DAG
        """
        # Verify that the AirflowPipelineProcessor supports the given export format
        self._verify_export_format(pipeline_export_format)

        timestamp = datetime.now().strftime("%m%d%H%M%S")
        # Create an instance id that will be used to store
        # the pipelines' dependencies, if applicable
        pipeline_instance_id = f"{pipeline.name}-{timestamp}"

        absolute_pipeline_export_path = get_absolute_path(self.root_dir, pipeline_export_path)

        if os.path.exists(absolute_pipeline_export_path) and not overwrite:
            raise ValueError(f"File '{absolute_pipeline_export_path}' already exists.")

        self.log_pipeline_info(pipeline.name, f"exporting pipeline as a .{pipeline_export_format} file")

        new_pipeline_file_path = self.create_pipeline_file(
            pipeline=pipeline,
            pipeline_export_format="py",
            pipeline_export_path=absolute_pipeline_export_path,
            pipeline_name=pipeline.name,
            pipeline_instance_id=pipeline_instance_id,
        )

        return new_pipeline_file_path


class IdomlPipelineProcessorResponse(RuntimePipelineProcessorResponse):
    _type = RuntimeProcessorType.IDOML
    _name = "idoml"

    def __init__(self, git_url, run_url, object_storage_url, object_storage_path):
        super().__init__(run_url, object_storage_url, object_storage_path)
        self.git_url = git_url

    def to_json(self):
        response = super().to_json()
        response["git_url"] = self.git_url
        return response
