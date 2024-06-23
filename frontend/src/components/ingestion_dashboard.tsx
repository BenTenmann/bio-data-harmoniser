"use client";
import React from "react";
import { usePathname } from "next/navigation";
import {
  BookOpenIcon,
  CheckCircleIcon,
  CubeTransparentIcon,
  CodeBracketIcon,
  XCircleIcon,
  StopCircleIcon,
  ArrowPathIcon,
} from "@heroicons/react/24/outline";
import { Badge } from "@/components/badge";
import { Button } from "@/components/button";
import { statusColors } from "@/lib/utils";
import Tabs from "@/components/tabs";

type Status = "success" | "failed" | "running";

function getStatusIcon(status: Status) {
  switch (status) {
    case "success":
      return <CheckCircleIcon className="h-6 w-6 text-lime-700" />;
    case "failed":
      return <XCircleIcon className="h-6 w-6 text-red-700" />;
    case "running":
      return <StopCircleIcon className="h-6 w-6 text-cyan-700" />;
    default:
      return null;
  }
}

export default function IngestionDashboard({ datum, children }) {
  const currentPath = usePathname();
  const reRun = async () => {
    const res = await fetch(
      "http://localhost:8080/api/v1/dags/data_extraction/dagRuns",
      {
        headers: {
          Authorization: "Basic " + btoa("admin:admin"),
          "Content-Type": "application/json",
        },
        method: "POST",
        body: JSON.stringify({
          conf: datum.conf,
        }),
      },
    );
    const jsonData = await res.json();
    console.log(jsonData);
  };
  return (
    <>
      <h1 className="text-2xl font-semibold leading-6 text-gray-900">
        {datum.name}
      </h1>
      <p className="mt-2 text-sm text-gray-500">{datum.description}</p>
      <div className="mt-4 flex items-center justify-between">
        <div>
          <Badge color={statusColors[datum.status]}>
            {getStatusIcon(datum.status)}
            {datum.status}
          </Badge>
        </div>
        <div>
          <Button color="light" onClick={reRun} href="#">
            <ArrowPathIcon /> Rerun
          </Button>
        </div>
      </div>
      <div>
        <Tabs
          tabs={[
            {
              name: "Tasks",
              href: `/dashboard/ingestion/${datum.id}/tasks`,
              icon: CubeTransparentIcon,
            },
            {
              name: "Mapping",
              href: `/dashboard/ingestion/${datum.id}/mapping`,
              icon: BookOpenIcon,
              disabled: datum.status === "running",
            },
            {
              name: "SQL",
              href: `/dashboard/ingestion/${datum.id}/sql`,
              icon: CodeBracketIcon,
              disabled: datum.status === "running",
            },
          ].map((tab) => {
            return {
              ...tab,
              current: currentPath === tab.href,
            };
          })}
        ></Tabs>
        {children}
      </div>
    </>
  );
}
