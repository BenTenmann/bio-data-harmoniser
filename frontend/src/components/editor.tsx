"use client";
import { useState } from "react";
import Editor from "@monaco-editor/react";

export default function CustomEditor({ code, setCode }) {
  return (
    <Editor
      defaultLanguage="sql"
      value={code}
      onChange={(value) => setCode(value!)}
      options={{
        minimap: {
          enabled: false,
        },
        hideCursorInOverviewRuler: true,
        overviewRulerBorder: false,
      }}
    />
  );
}
