# adapters/outbound/databases/bigquery_adapter.py

from typing import Any, Optional, List, Dict
import os
from google.cloud import bigquery
from datetime import datetime
from endpoints.database_query import QueryResult, TableInfo
from logger.gcp_logger import GCPLogger, LogLevel



class BigQueryService():
    """
    Pure BigQuery adapter that connects directly to BigQuery API.
    """

    def __init__(self, session_id: str, database_name:str,raise_on_error: bool = False,project_id:str="unidb-442214") -> None:
        """
        Initialize BigQuery adapter with direct BigQuery connection.

        Args:
            raise_on_error (bool): Whether to raise exceptions or log errors
        """
        self._raise_on_error = raise_on_error

        self._project_id = project_id

        self._database_name = database_name
        self._schema_tables = {}
        self._client = None
        if session_id is None:
            session_id = str(int(datetime.utcnow().timestamp()))
        self.session_id = session_id
        self._initialize_client()

    # Cached dictionary of fully-qualified table names in the current BigQuery database schema.
    # This property queries BigQuery for all table names on first access, then caches the result as a dict.
    # Usage: instance.schema_tables['conversion'] returns the full table reference for 'conversion'.
    # Useful for schema discovery and efficient repeated access to table references by name.
    @property
    def schema_tables(self) -> List[str]:
        if not self._schema_tables:
            query = f"SELECT table_name FROM `{self._project_id}.{self._database_name}.INFORMATION_SCHEMA.TABLES`"
            result = self.execute_query(query)
            if result.success:
                self._schema_tables = {
                row['table_name']: f"`{self._project_id}.{self._database_name}.{row['table_name']}`"
                for row in result.data
                }
        return self._schema_tables

    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        """
        Execute a SQL query and return structured results.

        Args:
            query (str): SQL query to execute
            parameters (Optional[Dict[str, Any]]): Query parameters for parameterized queries

        Returns:
            QueryResult: Structured query result with data, metadata, and success status
        """
        if not self._client:
            error_msg = "BigQuery client is not initialized"
            if self._raise_on_error:
                raise Exception(error_msg, "client_error")

            return QueryResult(
                data=[],
                row_count=0,
                success=False,
                error_message=error_msg,
                query=query,
                source=self._database_name
            )

        try:
            # Configure query job
            job_config = bigquery.QueryJobConfig()

            # Add query parameters if provided
            if parameters is not None and parameters != {}:
                job_config.query_parameters = self._convert_parameters(parameters)

            # Execute query directly via BigQuery API
            query_job = self._client.query(query, job_config=job_config)
            #throw error if exist
            if query_job.errors:
                raise Exception(f"BigQuery error: {query_job.errors} query: {query}")

            # Wait for query to complete and get results
            results = query_job.result()

            # Convert to list of dictionaries
            data = [dict(row) for row in results]

            return QueryResult(
                data=data,
                row_count=len(data),
                success=True,
                query=query,
                source=self._database_name,
                execution_time_ms=query_job.ended - query_job.started if query_job.ended and query_job.started else None
            )

        except Exception as e:
            error_msg = getattr(e, "message", None)
            if not error_msg:
                error_msg = f"Query execution failed: {str(e)}"

            if self._raise_on_error:
                raise Exception(error_msg, "query_error") from e

            GCPLogger.log(LogLevel.ERROR, "BigQuery-Service", {
                "session_id": self.session_id,
                "message": error_msg
            })
            return QueryResult(
                data=[],
                row_count=0,
                success=False,
                error_message=error_msg,
                query=query,
                source=self._database_name
            )


    def _initialize_client(self) -> None:
        """
        Initialize BigQuery client directly without using the old connector.
        """
        try:
            from google.cloud import bigquery
            from google.oauth2.service_account import Credentials


            # Get credentials from service account file
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'private/automations-and-plugins-6af5a4621ff9.json')

            if os.path.exists(credentials_path):
                # Use service account credentials
                credentials = Credentials.from_service_account_file(credentials_path)

                # Initialize BigQuery client
                self._client = bigquery.Client(
                    credentials=credentials,
                    project=self._project_id
                )

            else:
                # Try default credentials (useful in GCP environments)
                try:
                    self._client = bigquery.Client(project=self._project_id)
                    GCPLogger.log(LogLevel.INFO, "BigQuery-Service", {
                        "session_id": self.session_id,
                        "message": f"BigQuery client initialized with default credentials for {self._database_name}"
                    })
                except Exception as default_cred_error:
                    error_msg = f"BigQuery credentials not found. Tried: {credentials_path} and default credentials"
                    GCPLogger.log(LogLevel.ERROR, "BigQuery-Service", {
                        "session_id": self.session_id,
                        "message": error_msg
                    })

                    if self._raise_on_error:
                        raise Exception(error_msg, "credentials_missing") from default_cred_error

        except ImportError as e:
            error_msg = f"Google Cloud BigQuery library not installed: {e}"
            GCPLogger.log(LogLevel.ERROR, "BigQuery-Service", {
                "session_id": self.session_id,
                "message": error_msg
            })

            if self._raise_on_error:
                raise Exception(error_msg, "library_missing") from e

        except Exception as e:
            error_msg = f"Failed to initialize BigQuery client: {str(e)}"
            GCPLogger.log(LogLevel.ERROR, "BigQuery-Service", {
                "session_id": self.session_id,
                "message": error_msg
            })

            if self._raise_on_error:
                raise Exception(error_msg, "initialization_error") from e

    def _convert_parameters(self, parameters: Dict[str, Any]) -> List:
        """
        Convert parameter dictionary to BigQuery query parameters format.

        Args:
            parameters (Dict[str, Any]): Parameter dictionary

        Returns:
            List: BigQuery query parameters
        """
        from google.cloud import bigquery

        query_parameters = []
        for key, value in parameters.items():
            if isinstance(value, list):
                value = [str(v) for v in value]
                query_parameters.append(
                    bigquery.ArrayQueryParameter(key, bigquery.SqlTypeNames.STRING, value)
                )
                continue
            # Determine parameter type based on value
            if isinstance(value, str):
                param_type = bigquery.SqlTypeNames.STRING
            elif isinstance(value, int):
                param_type = bigquery.SqlTypeNames.INT64
            elif isinstance(value, float):
                param_type = bigquery.SqlTypeNames.FLOAT64
            elif isinstance(value, bool):
                param_type = bigquery.SqlTypeNames.BOOL
            else:
                param_type = bigquery.SqlTypeNames.STRING
                value = str(value)

            query_parameters.append(
                bigquery.ScalarQueryParameter(key, param_type, value)
            )

        return query_parameters