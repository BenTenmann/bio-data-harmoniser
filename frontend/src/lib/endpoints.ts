const AIRFLOW_HOST = process.env.AIRFLOW_HOST ?? "localhost";
const AIRFLOW_PORT = process.env.AIRFLOW_PORT ?? "8080";

const FASTAPI_HOST = process.env.FASTAPI_HOST ?? "localhost";
const FASTAPI_PORT = process.env.FASTAPI_PORT ?? "80";

const AIRFLOW_BASE_URL = `http://${AIRFLOW_HOST}:${AIRFLOW_PORT}`;
const FASTAPI_BASE_URL = `http://${FASTAPI_HOST}:${FASTAPI_PORT}`;

export const endpoints: {
    dataExtractionDagRuns: string;
    sqlQueryDagRuns: string;
    sql: string;
    mappings: string;
    secrets: string;
    dataTypes: string;
    ingestions: string;
    catalog: string;
    dag: string;
    paperIngestionMetadata: string;
    ingestionFileUpload: string;
    schemas: string;
    entities: string;
} = {
    // TODO: fix this
    // currently broken in client components
    dataExtractionDagRuns: `${AIRFLOW_BASE_URL}/api/v1/dags/data_extraction/dagRuns`,
    sqlQueryDagRuns: `${AIRFLOW_BASE_URL}/api/v1/dags/sql_query/dagRuns`,
    sql: `${FASTAPI_BASE_URL}/sql`,
    mappings: `${FASTAPI_BASE_URL}/mappings`,
    secrets: `${FASTAPI_BASE_URL}/secrets`,
    dataTypes: `${FASTAPI_BASE_URL}/data-types`,
    ingestions: `${FASTAPI_BASE_URL}/ingestions`,
    catalog: `${FASTAPI_BASE_URL}/catalog`,
    dag: `${FASTAPI_BASE_URL}/dag`,
    paperIngestionMetadata: `${FASTAPI_BASE_URL}/paper-ingestion/metadata`,
    ingestionFileUpload: `${FASTAPI_BASE_URL}/ingestion/file-upload`,
    schemas: `${FASTAPI_BASE_URL}/schemas`,
    entities: `${FASTAPI_BASE_URL}/entities`,
};
