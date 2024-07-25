import {
  CreateSchema as CreateSchemaComponent,
  type DataType,
} from "@/components/create_schema";
import { endpoints } from "@/lib/endpoints";

export const dynamic = "force-dynamic";

async function getDataTypes(): Promise<DataType[]> {
  const response = await fetch(endpoints.dataTypes);
  if (!response.ok) {
    throw new Error("Failed to fetch data types");
  }
  return await response.json();
}

async function getStringDataType(): Promise<DataType> {
  const response = await fetch(`${endpoints.dataTypes}/STRING`);
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
