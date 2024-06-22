"use client";
import React from "react";
import CustomEditor from "@/components/editor";
import { Text } from "@/components/text";
import {
  getQueryStatus,
  getSqlResults,
  submitSql,
} from "@/lib/sql";
import { QueryStatus } from "@/lib/utils";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
} from "@/components/table";
import { Button } from "@/components/button";
import { PlayIcon } from "@heroicons/react/24/solid";

type Table = {
  index: any[];
  columns: string[];
  data: any[];
};

function TableView({ data }: { data: Table }) {
  return (
    <Table
      className="[--gutter:theme(spacing.6)] sm:[--gutter:theme(spacing.8)]"
      dense
    >
      <TableHead>
        <TableRow>
          {data.columns.map((column) => (
            <TableCell key={column}>{column}</TableCell>
          ))}
        </TableRow>
      </TableHead>
      <TableBody>
        {data.data.map((row, row_index) => (
          <TableRow key={row_index}>
            {row.map((datum: any, col_index: number) => (
              <TableCell key={`${row_index}-${col_index}`}>{datum}</TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

export function SqlQuery({ runId }: { runId: string }) {
  // TODO: things to do:
  // - fix command+enter to run
  // - fix the table to be responsive and not overflow
  // - add pagination for the table
  // - add a button to download the results
  const [code, setCode] = React.useState("-- Write your SQL here");
  const [isRunning, setIsRunning] = React.useState(false);
  const [data, setData] = React.useState<Table | undefined>(undefined);

  const doSubmit = async () => {
    const queryId = await submitSql(code, runId);
    setIsRunning(true);
    await pollingTaskStatus(queryId);
  };

  const handleSubmit = async (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      await doSubmit();
    }
  };
  const pollingTaskStatus = async (queryId: string) => {
    const interval = setInterval(async () => {
      const status = await getQueryStatus(queryId);
      if (status === QueryStatus.SUCCESS) {
        const res = await getSqlResults(queryId);
        setData(res);
        setIsRunning(false);
        clearInterval(interval);
      } else if (status === QueryStatus.FAILED) {
        setIsRunning(false);
        clearInterval(interval);
      }
    }, 1000);
  };
  return (
    <div className="flex h-64 flex-col">
      <div className="h-1/2" onKeyDown={handleSubmit} tabIndex={0}>
        <CustomEditor code={code} setCode={setCode} />
      </div>
      <div className="h-1/2 border-t border-zinc-950/10 pt-2">
        <div className="flex justify-center">
          <div className="text-center">
            {isRunning ? (
              <Text color="zinc-500">Running...</Text>
            ) : data === undefined ? (
              <Text color="zinc-500">Press Ctrl+Enter to run</Text>
            ) : (
              <TableView data={data} />
            )}
          </div>
        </div>
        <div className="flex justify-center">
          <Button onClick={() => doSubmit()} disabled={isRunning}>
            <PlayIcon className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}
