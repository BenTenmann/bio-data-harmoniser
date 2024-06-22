import {
  CreateSchema as CreateSchemaComponent,
  type DataType,
} from "@/components/create_schema";

export const dynamic = "force-dynamic";

async function getDataTypes(): Promise<DataType[]> {
  const response = await fetch("http://0.0.0.0:80/data-types");
  if (!response.ok) {
    throw new Error("Failed to fetch data types");
  }
  return await response.json();
}

async function getStringDataType(): Promise<DataType> {
  const response = await fetch("http://0.0.0.0:80/data-types/STRING");
  if (!response.ok) {
    throw new Error("Failed to fetch string data type");
  }
  return await response.json();
}

export default async function CreateSchema() {
  const dataTypes = await getDataTypes();
  const stringDataType = await getStringDataType();
  return (
    <CreateSchemaComponent
      dataTypes={dataTypes}
      stringDataType={stringDataType}
    />
  );
}
