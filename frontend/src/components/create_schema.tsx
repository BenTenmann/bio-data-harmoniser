"use client";
import React from "react";
import Shell from "@/components/shell";
import {
  Field,
  FieldGroup,
  Fieldset,
  Label,
  Legend,
} from "@/components/fieldset";
import { Text } from "@/components/text";
import { Input } from "@/components/input";
import { Textarea } from "@/components/textarea";
import { PlusIcon, MinusIcon } from "@heroicons/react/20/solid";
import { Listbox, ListboxOption } from "@/components/listbox";
import { Button } from "@/components/button";
import { useRouter } from "next/navigation";
import * as Headless from "@headlessui/react";
import { Radio } from "@/components/radio";

export type DataTypeParamOption = {
  name: string;
  value: any;
  description?: string;
};

export type DataTypeParam = {
  key: string;
  name: string;
  options: DataTypeParamOption[];
  allow_multiple: boolean;
  options_ordered: boolean;
  default?: string;
  choice?: DataTypeParamOption;
};

export type DataType = {
  key: string;
  name: string;
  description: string;
  parameters?: DataTypeParam[];
};

export type ColumnType = {
  name: string;
  description: string;
  data_type: DataType;
};

function SchemaColumn({
  column,
  index,
  disabled,
  removeColumn,
  handleColumnChange,
  handleColumnDataTypeChange,
  dataTypes,
  dataTypesLookup,
}: {
  column: ColumnType;
  index: number;
  disabled: boolean;
  removeColumn: (index: number) => void;
  handleColumnChange: (index: number, propName: string, value: any) => void;
  handleColumnDataTypeChange: (columnIndex: number, paramIndex: number, choice: DataTypeParamOption) => void;
  dataTypes: DataType[];
  dataTypesLookup: { [key: string]: DataType };
}) {
  return (
    <FieldGroup>
      <div className="flex justify-between gap-x-4">
        <div className="grid w-full grid-cols-1 gap-8 p-2 sm:grid-cols-3 sm:gap-4">
          <Field>
            <Label>Name</Label>
            <Input
              type="text"
              defaultValue={column.name}
              name={`field_name_${index}`}
              disabled={disabled}
              onChange={(e) => handleColumnChange(index, "name", e.target.value)}
            />
          </Field>
          <Field>
            <Label>Data Type</Label>
            <Listbox
              disabled={disabled}
              defaultValue={column.data_type.key}
              onChange={(value) =>
                handleColumnChange(index, "data_type", dataTypesLookup[value])
              }
            >
              {dataTypes.map((dtype, index) => (
                <ListboxOption key={index} value={dtype.key}>
                  {dtype.name}
                </ListboxOption>
              ))}
            </Listbox>
            {column.data_type.parameters && (
              <div className="rounded-b-xl border-b border-l border-r border-zinc-950/10 p-2 text-sm text-gray-500">
                {column.data_type.parameters.map((param, paramIndex) => (
                  <Field key={paramIndex} className="p-2">
                    <Label>{param.name}</Label>
                    {!param.options_ordered ? (
                      <Listbox onChange={(e) => handleColumnDataTypeChange(
                          index,
                          paramIndex,
                          param.options.find((o) => o.name === e))}>
                        {param.options.map((option, index) => (
                          <ListboxOption key={index} value={option.name}>
                            {option.name}
                          </ListboxOption>
                        ))}
                      </Listbox>
                    ) : (
                      <Headless.RadioGroup
                        className="mt-4 flex gap-6 sm:gap-8"
                        key={paramIndex}
                        defaultValue={param.default}
                        onChange={(e) => handleColumnDataTypeChange(
                          index,
                          paramIndex,
                          param.options.find((o) => o.name === e))}
                      >
                        {param.options.map((option, index) => (
                          <Headless.Field
                            key={`${index}_${option.name}`}
                            className="flex items-center gap-2"
                          >
                            <Radio value={option.name} />
                            <Headless.Label>{option.name}</Headless.Label>
                          </Headless.Field>
                        ))}
                      </Headless.RadioGroup>
                    )}
                  </Field>
                ))}
              </div>
            )}
          </Field>
          <Field>
            <Label>Description</Label>
            <Input
              type="text"
              defaultValue={column.description}
              name={`field_description_${index}`}
              disabled={disabled}
              onChange={(e) =>
                handleColumnChange(index, "description", e.target.value)
              }
            />
          </Field>
        </div>
        <div className="relative w-1/12">
          <div className="absolute bottom-0 right-0">
            <Button
              color="light"
              onClick={() => removeColumn(index)}
              disabled={disabled}
            >
              <MinusIcon />
            </Button>
          </div>
        </div>
      </div>
    </FieldGroup>
  );
}

export function CreateSchema({
  dataTypes,
  stringDataType,
}: {
  dataTypes: DataType[];
  stringDataType: DataType;
}) {
  const dataTypesLookup = React.useMemo(() => {
    return dataTypes.reduce(
      (acc, dtype) => {
        acc[dtype.key] = dtype;
        return acc;
      },
      {} as { [key: string]: DataType },
    );
  }, [dataTypes]);
  const router = useRouter();
  const [formData, setFormData]: [
    {
      schema_name: string;
      schema_description: string;
      columns: ColumnType[];
    },
    React.Dispatch<
      React.SetStateAction<{
        schema_name: string;
        schema_description: string;
        columns: ColumnType[];
      }>
    >,
  ] = React.useState({
    schema_name: "",
    schema_description: "",
    columns: [] as ColumnType[],
  });
  const [columns, setColumns]: [
    ColumnType[],
    React.Dispatch<React.SetStateAction<ColumnType[]>>,
  ] = React.useState([
    {
      name: "dataset_id",
      data_type: stringDataType,
      description: "The dataset ID.",
    },
    {
      name: "",
      data_type: stringDataType,
      description: "",
    },
  ]);

  const addColumn = () => {
    setColumns([
      ...columns,
      {
        name: "",
        data_type: stringDataType,
        description: "",
      },
    ]);
  };
  React.useEffect(() => {
    setFormData((prevState) => ({
      ...prevState,
      columns: columns,
    }));
  }, [columns]);

  const removeColumn = (index: number) => {
    console.log(index);
    setColumns(columns.filter((_, i) => i !== index));
  };

  const handleColumnChange = (index: number, propName: string, value: any) => {
    setColumns(
      columns.map((column, i) => {
        if (i === index) {
          return {
            ...column,
            [propName]: value,
          };
        }
        return column;
      }),
    );
  };

  const handleColumnDataTypeChange = (
      columnIndex: number,
      paramIndex: number,
      choice: DataTypeParamOption
    ) => {
    setColumns(
        columns.map((column, i) => {
          if (columnIndex === i) {
            return {
              ...column,
              data_type: {
                ...column.data_type,
                parameters: column.data_type.parameters!.map(
                    (param, j) => {
                      if (paramIndex === j) {
                        return {
                          ...param,
                          choice: choice
                        }
                      }
                      return param;
                    }
                )
              }
            }
          }
          return column;
        })
    )
  }

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const { name, value } = e.target;
    setFormData((prevState) => ({
      ...prevState,
      [name]: value,
    }));
  };

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    console.log(formData);
    const res = await fetch(
      "http://0.0.0.0:80/schemas",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: formData.schema_name,
          description: formData.schema_description,
          columns: formData.columns
        }),
      },
    );
    if (res.ok) {
      console.log("Schema created");
      router.push("/dashboard/data-model");
    } else {
      console.error("Failed to create schema");
      console.error(res.status);
      console.error(await res.text());
    }
  };
  return (
    <>
      <form onSubmit={handleSubmit}>
        <Fieldset>
          <Legend>Create Schema</Legend>
          <Text>Create a new Data Model schema.</Text>
          <FieldGroup>
            <Field>
              <Label>Name</Label>
              <Input
                type="text"
                placeholder="Name"
                name="schema_name"
                onChange={handleChange}
              />
            </Field>
            <Field>
              <Label>Description</Label>
              <Textarea
                placeholder="Description"
                name="schema_description"
                onChange={handleChange}
              />
            </Field>
          </FieldGroup>
          <div className="mt-4 flex flex-col gap-y-4 divide-y">
            {columns.map((column, index) => (
              <SchemaColumn
                key={index}
                column={column}
                index={index}
                disabled={index === 0}
                removeColumn={removeColumn}
                handleColumnChange={handleColumnChange}
                handleColumnDataTypeChange={handleColumnDataTypeChange}
                dataTypes={dataTypes}
                dataTypesLookup={dataTypesLookup}
              />
            ))}
            <div className="flex justify-start gap-x-4 pt-4">
              <Button onClick={addColumn} color="light">
                <PlusIcon />
              </Button>
            </div>
          </div>
        </Fieldset>
        <div className="mt-4 flex justify-end gap-x-4">
          <Button outline href="/dashboard/data-model">
            Cancel
          </Button>
          <Button type="submit">Create</Button>
        </div>
      </form>
    </>
  );
}
