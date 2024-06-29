"use client";
import React from "react";
import {
  Field,
  FieldGroup,
  Fieldset,
  Label,
  Legend,
} from "@/components/fieldset";
import { useRouter } from "next/navigation";
import { Text } from "@/components/text";
import { Input } from "@/components/input";
import { Textarea } from "@/components/textarea";
import { ArrowUpTrayIcon } from "@heroicons/react/20/solid";
import { Button } from "@/components/button";
import { Radio } from "@/components/radio";
import * as Headless from "@headlessui/react";

type IngestionType = "URL" | "File Upload";

function FileUpload({
  file,
  handleFileChange,
}: {
  file: File | null;
  handleFileChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
}) {
  return (
    <Field>
      <div className="col-span-full">
        <div className="mt-2 flex justify-center rounded-lg border border-dashed border-gray-900/25 px-6 py-10">
          {file === null ? (
            <div className="text-center">
              <ArrowUpTrayIcon
                className="mx-auto h-12 w-12 text-gray-300"
                aria-hidden="true"
              />
              <div className="mt-4 flex text-sm leading-6 text-gray-600">
                <label
                  htmlFor="file-upload"
                  className="relative cursor-pointer rounded-md bg-white font-semibold text-indigo-600 focus-within:outline-none focus-within:ring-2 focus-within:ring-indigo-600 focus-within:ring-offset-2 hover:text-indigo-500"
                >
                  <span>Upload a file</span>
                  <input
                    id="file-upload"
                    name="file-upload"
                    type="file"
                    className="sr-only"
                    onChange={handleFileChange}
                  />
                </label>
                <p className="pl-1">or drag and drop</p>
              </div>
              <p className="text-xs leading-5 text-gray-600">
                CSV, TSV, PARQUET up to 200MB
              </p>
            </div>
          ) : (
            <div className="text-center">
              <p className="text-sm leading-6 text-gray-600">{file.name}</p>
            </div>
          )}
        </div>
      </div>
    </Field>
  );
}

export default function CreateIngestionPage() {
  const [formData, setFormData] = React.useState({
    name: "",
    description: "",
    url: "",
    user_id: "test_user",
  });
  const [ingestionType, setIngestionType]: [
      undefined | IngestionType,
      React.Dispatch<React.SetStateAction<undefined | IngestionType>>,
  ] = React.useState(undefined as undefined | IngestionType);
  const [file, setFile] = React.useState<File | null>(null);
  const router = useRouter();

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const { name, value } = e.target;
    setFormData((prevState) => ({
      ...prevState,
      [name]: value,
    }));
  };

  const handleUrlChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const { value } = e.target;
    const res = await fetch(`http://0.0.0.0:80/paper-ingestion/metadata`, {
      headers: {
        "Content-Type": "application/json",
      },
      method: "POST",
      body: JSON.stringify({
        url: value,
      }),
    });
    const updateData: { url: string; name?: string; description?: string } = {
      url: value,
    };
    if (res.status === 200) {
      const jsonData = await res.json();
      if (formData.name === "") {
        updateData.name = jsonData.name;
      }
      if (formData.description === "") {
        updateData.description = jsonData.description;
      }
    }
    setFormData((prevState) => ({
      ...prevState,
      ...updateData,
    }));
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const { files } = e.target;
    const file = files![0];
    const fileFormData = new FormData();
    fileFormData.append("file", file);
    const response = await fetch(`http://0.0.0.0:80/ingestion/file-upload`, {
      method: "POST",
      body: fileFormData,
    });
    const jsonData: { url: string } = await response.json();
    setFormData((prevState) => ({
      ...prevState,
      url: jsonData.url,
    }));
    setFile(file);
  };

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const res = await fetch(
      "http://localhost:8080/api/v1/dags/data_extraction/dagRuns",
      {
        headers: {
          Authorization: "Basic " + btoa("admin:admin"),
          "Content-Type": "application/json",
        },
        method: "POST",
        body: JSON.stringify({
          conf: formData,
        }),
      },
    );
    if (res.ok) {
      console.log("Ingestion created");
      router.push("/dashboard/ingestion");
    } else {
      console.error("Failed to create ingestion");
      console.error(res.status);
      console.error(await res.text());
    }
  };
  return (
    <>
      <form onSubmit={handleSubmit}>
        <Fieldset>
          <Legend>Create Ingestion</Legend>
          <Text>Create a new ingestion pipeline from a URL or a file.</Text>
          <FieldGroup>
            <Field>
              <Label>Ingestion Type</Label>
              <Headless.RadioGroup
                name="ingestion_type"
                className="mt-4 flex gap-6 sm:gap-8"
                onChange={(e: IngestionType) => setIngestionType(e)}
              >
                {["URL", "File Upload"].map((type) => (
                  <Headless.Field
                    key={type}
                    className="flex items-center gap-2"
                  >
                    <Radio value={type} />
                    <Headless.Label>{type}</Headless.Label>
                  </Headless.Field>
                ))}
              </Headless.RadioGroup>
            </Field>
          </FieldGroup>
          {ingestionType ? (
            <FieldGroup>
              <FieldGroup>
                <Field>
                  <Label>Name</Label>
                  <Input
                    type="text"
                    placeholder="Name"
                    name="name"
                    onChange={handleChange}
                    value={formData.name}
                  />
                </Field>
                <Field>
                  <Label>Description</Label>
                  <Textarea
                    placeholder="Description"
                    name="description"
                    onChange={handleChange}
                    value={formData.description}
                  />
                </Field>
              </FieldGroup>
              <FieldGroup>
                {ingestionType === "File Upload" ? (
                  <FileUpload file={file} handleFileChange={handleFileChange} />
                ) : (
                  <Field>
                    <Label>Source</Label>
                    <Input
                      type="text"
                      placeholder="URL"
                      name="url"
                      onChange={handleUrlChange}
                    />
                  </Field>
                )}
              </FieldGroup>
            </FieldGroup>
          ) : null}
        </Fieldset>
        <div className="mt-4 flex justify-end gap-x-4">
          <Button outline href="/">
            Cancel
          </Button>
          <Button type="submit" disabled={ingestionType === undefined}>
            Create
          </Button>
        </div>
      </form>
    </>
  );
}
