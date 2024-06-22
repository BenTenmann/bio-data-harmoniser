"use client";
import React from "react";
import {
  CloudArrowDownIcon,
  BookOpenIcon,
  ClipboardDocumentListIcon,
  LinkIcon,
  CheckCircleIcon,
  XCircleIcon,
  StopCircleIcon,
  ExclamationCircleIcon,
  CircleStackIcon,
  ArrowLongRightIcon,
  ArrowsPointingInIcon,
} from "@heroicons/react/24/outline";
import {
  QuestionMarkCircleIcon
} from "@heroicons/react/20/solid";
import { Handle, Position } from "reactflow";
import { type Mapping } from "@/lib/mapping_table";
import { Dialog, DialogActions, DialogBody, DialogDescription, DialogTitle } from "@/components/dialog";
import { Code } from "@/components/text";
import {Button} from "@/components/button";
import { Link } from "@/components/link";

type RenameOperation = {
  original_name: string;
  new_name: string;
}

type MappingType = "free_text" | "xref";

type MappingOperation = {
  type: MappingType;
  mappings: Mapping[];
}

type DerivedInferenceOperation = {
  type: "derived";
  data: null;
}

type RagResponse = {
  answer: string;
  references: {
    text: string;
    url?: string;
  }
}

type ExtractedInferenceOperation = {
  type: "extracted";
  data: RagResponse;
}

type InferenceOperation = DerivedInferenceOperation | ExtractedInferenceOperation;

type SetValueOperation = {
  value: any;
}

type Operation =
  | RenameOperation
  | MappingOperation
  | InferenceOperation
  | SetValueOperation;

function isRenameOp(op: Operation): op is RenameOperation {
  return (op as RenameOperation).original_name !== undefined;
}

function isMappingOp(op: Operation): op is MappingOperation {
  return (op as MappingOperation).mappings !== undefined;
}

function isInferenceOp(op: Operation): op is InferenceOperation {
  return (op as InferenceOperation).data !== undefined;
}

function isSetValueOp(op: Operation): op is SetValueOperation {
  return (op as SetValueOperation).value !== undefined;
}

type ColumnAlignment = {
  column_name: string;
  operations: Operation[];
}

type Decision = {
  type: string;
  content: string | ColumnAlignment;
}

export type TaskType = "retrieve" | "download" | "extract" | "process";

export type NodeMetadata = {
  name: string;
  status: string;
  type: TaskType;
  logs: string[];
  decisions: Decision[];
  duration: number;
}

const iconLookup = {
  retrieve: LinkIcon,
  download: CloudArrowDownIcon,
  extract: BookOpenIcon,
  pool: ArrowsPointingInIcon,
  process: ClipboardDocumentListIcon,
};

function StatusIcon({ status }: { status: string }) {
  const size = "h-6 w-6";
  const color = "text-gray-500";
  switch (status) {
    case "success":
      return <CheckCircleIcon className={`${size} ${color}`} />;
    case "failed":
      return <XCircleIcon className={`${size} ${color}`} />;
    case "running":
      return <StopCircleIcon className={`${size} ${color}`} />;
    case "skipped":
      return <ExclamationCircleIcon className={`${size} ${color}`} />;
    default:
      return <ExclamationCircleIcon className={`${size} ${color}`} />;
  }
}

function NodeIcon({ type, color }: { type: TaskType; color: string }) {
  const Icon = iconLookup[type];
  return <Icon className={`h-6 w-6 ${color}`} />;
}

function DecisionIcon({ type }: { type: string }) {
  const size = "h-4 w-4";
  const color = "text-gray-500 hover:text-indigo-500";
  switch (type) {
    case "column_aligned":
      return <CircleStackIcon className={`${size} ${color}`} />;
    default:
      return <ExclamationCircleIcon className={`${size} ${color}`} />;
  }
}

function InferenceComponent({ inference }: { inference: InferenceOperation }) {
  switch (inference.type) {
    case "derived":
      return <div className="flex flex-row items-center space-x-2">
        <span className="text-sm">The column was derived from other columns</span>
      </div>;
    case "extracted":
      return <div className="flex flex-row items-center space-x-2">
        <span className="text-sm">The column was extracted from the context: </span>
        <span className="text-sm truncate">{inference.data.answer}</span>
          {/*{*/}
          {/*  inference.data.references.url ? (*/}
          {/*    <Link href={inference.data.references.url}>*/}
          {/*      <span className="text-sm">(see {inference.data.references.text})</span>*/}
          {/*    </Link>*/}
          {/*  ) : (*/}
          {/*    <span className="text-sm">(see {inference.data.references.text})</span>*/}
          {/*  )*/}
          {/*}*/}
        <QuestionMarkCircleIcon className="h-4 w-4"/>
      </div>;
    default:
      return <div className="flex flex-row items-center space-x-2">
        <span className="text-sm">unknown inference type</span>
      </div>;
  }
}

function DecisionComponent({ decision, runId }: { decision: Decision, runId: string }) {
  const [showDialog, setShowDialog] = React.useState(false);
  return (
    <div className="flex flex-row items-center space-x-2 hover:cursor-pointer hover:text-indigo-500" onClick={() => setShowDialog(true)}>
      <DecisionIcon type={decision.type} />
      <span className="text-sm">
        {
          typeof decision.content === "string" ? decision.content : (
              <div className="flex flex-row items-center space-x-2">
                <span className="text-sm">{decision.content.column_name}</span>
                {/*<span className="text-sm">{decision.content.value}</span>*/}
                <Dialog open={showDialog} onClose={() => setShowDialog(false)}>
                  <DialogTitle>alignment of column <Code>{decision.content.column_name}</Code></DialogTitle>
                  <DialogDescription>
                    {
                      decision.content.operations.map((op, index) => (
                        isRenameOp(op) ? (
                          <div key={index} className="flex flex-row items-center space-x-2">
                            <span className="text-sm">Column was renamed: </span>
                            <span className="text-sm">{op.original_name}</span>
                            <ArrowLongRightIcon className="h-4 w-4" />
                            <span className="text-sm">{op.new_name}</span>
                          </div>
                        ) : isMappingOp(op) ? (
                          <div key={index} className="flex flex-row items-center space-x-2">
                            <span className="text-sm">Column values were mapped (<Link href={`/ingestion/${runId}/mapping`}>see here</Link>): </span>
                            <span className="text-sm">{op.mappings.length} unique mappings</span>
                          </div>
                        ) : isInferenceOp(op) ? (
                          InferenceComponent({ inference: op })
                        ) : isSetValueOp(op) ? (
                          <div key={index} className="flex flex-row items-center space-x-2">
                            <span className="text-sm">Column values were set to default: </span>
                            <span className="text-sm"><Code>{String(op.value)}</Code></span>
                          </div>
                        ) : (
                          <div key={index} className="flex flex-row items-center space-x-2">
                            <span className="text-sm">unknown operation</span>
                          </div>
                        )
                      ))
                    }
                  </DialogDescription>
                  <DialogActions>
                    <Button outline onClick={() => setShowDialog(false)}>Close</Button>
                  </DialogActions>
                </Dialog>
              </div>
          )
        }
      </span>
    </div>
  );
}

export const CustomNode = ({ data }: { data: NodeMetadata & { runId: string } }) => {
  const [selected, setSelected] = React.useState(false);
  return (
    <>
      <div
        className={`rounded-lg border-2 bg-white p-5 ${selected ? "border-solid border-indigo-500 text-indigo-500" : "border-dashed border-gray-400"} shadow-sm`}
        onClick={() => setSelected(!selected)}
      >
        <div className="flex items-center space-x-2">
          <NodeIcon
            type={data.type}
            color={selected ? `text-indigo-500` : `text-gray-900`}
          />
          <span
            className={`text-lg font-semibold ${selected ? "text-indigo-500 drop-shadow-glow" : "text-gray-900"}`}
          >
            {data.name}
          </span>
        </div>
      </div>
      {
        selected && (
          <div className="flex flex-col items-center space-x-2 bg-gray-100 p-2 rounded-b-lg border-b border-gray-400">
            <div className="flex flex-row items-center space-x-2">
              <StatusIcon status={data.status} />
              <span className="text-sm">in {data.duration}s</span>
            </div>
            <ul className="flex flex-col">
              {
                data.decisions.map((decision, index) => (
                  <div key={index} className="flex flex-row items-center space-x-2">
                    <DecisionComponent decision={decision} runId={data.runId} />
                  </div>
                ))
              }
            </ul>
          </div>
        )
      }
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
    </>
  );
};
