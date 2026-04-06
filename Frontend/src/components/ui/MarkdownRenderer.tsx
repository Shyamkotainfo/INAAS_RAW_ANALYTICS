"use client";

import React from "react";
import ReactMarkdown, { Components } from "react-markdown";

const markdownComponents: Components = {
  li: ({ children, ...props }) => (
    <li className="mb-1" {...props}>
      {children}
    </li>
  ),
  strong: ({ children, ...props }) => (
    <strong className="text-foreground" {...props}>
      {children}
    </strong>
  ),
  em: ({ children, ...props }) => (
    <em className="text-foreground/80" {...props}>
      {children}
    </em>
  ),
};

interface MarkdownRendererProps {
  text: string;
  className?: string;
}

export default function MarkdownRenderer({ text }: MarkdownRendererProps) {
  return (
    <ReactMarkdown
    //   className={className}
      components={markdownComponents}
    >
      {text}
    </ReactMarkdown>
  );
}
