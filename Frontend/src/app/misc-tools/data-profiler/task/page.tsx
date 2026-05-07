"use client";

import { AppLayout } from "@/components/layout/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ArrowLeft, Database, BarChart3, ChevronDown, TableIcon, Sparkles, User, Calendar, File, Plus } from "lucide-react";
import { useRouter, useSearchParams } from "next/navigation";
import { useState, useEffect, ReactNode, useRef } from "react";
import { apiService } from "@/services/apiService";
import DeepExplorerInsight, { ResponseEntry } from "@/components/DeepExplorer";

interface ColumnProfile {
  column_name: string;
  data_type: string;
  nullable: boolean;
  null_count: number;
  null_percentage: number;
  distinct_count: number;
  min?: string;
  max?: string;
  mean?: number;
  sample_values?: (string | number | null)[];
}

interface ProfilingResponse {
  success: boolean;
  dataset_id: string;
  overview?: string[];
  profiling: {
    row_count: number;
    column_count: number;
    duplicate_rows?: number;
    columns: ColumnProfile[];
  };
}

interface ProfileUploadResponse {
  success: boolean;
  dataset_id: string;
  file_path: string;
  file_format: string;
}

function getStoredUploadData(): ProfileUploadResponse | null {
  const stored = sessionStorage.getItem("profilingUpload");
  if (!stored) return null;

  try {
    return JSON.parse(stored) as ProfileUploadResponse;
  } catch {
    return null;
  }
}

interface CollapsiblePanelProps {
  title: string;
  defaultOpen?: boolean;
  badge?: ReactNode;
  children: ReactNode;
}

function CollapsiblePanel({ title, defaultOpen = true, badge, children }: CollapsiblePanelProps) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <Card>
        <CollapsibleTrigger asChild>
          <CardHeader className="cursor-pointer select-none hover:bg-muted/50 transition-colors">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <ChevronDown className={`w-4 h-4 text-muted-foreground transition-transform ${open ? "" : "-rotate-90"}`} />
                <CardTitle className="text-lg">{title}</CardTitle>
              </div>
              {badge}
            </div>
          </CardHeader>
        </CollapsibleTrigger>

        <CollapsibleContent>
          <CardContent>{children}</CardContent>
        </CollapsibleContent>
      </Card>
    </Collapsible>
  );
}

export default function ExplorationTaskDetail() {

  const router = useRouter();
  const searchParams = useSearchParams();

  const [data, setData] = useState<ProfilingResponse | null>(null);
  const [uploadData, setUploadData] = useState<ProfileUploadResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [responses, setResponses] = useState<ResponseEntry[]>([]);

  const fetchedRef = useRef(false);

  useEffect(() => {

    (async function () {

      if (fetchedRef.current) return;
      fetchedRef.current = true;

      const uploadFromQuery = {
        success: true,
        dataset_id: searchParams.get("dataset_id") ?? "",
        file_path: searchParams.get("file_path") ?? "",
        file_format: searchParams.get("file_format") ?? "",
      };
      const storedUpload = getStoredUploadData();
      const profiling =
        uploadFromQuery.dataset_id &&
          uploadFromQuery.file_path &&
          uploadFromQuery.file_format
          ? uploadFromQuery
          : storedUpload;
      const storedDomain = sessionStorage.getItem("profilingDomain");
      const domain = storedDomain ? storedDomain : undefined;
      setUploadData(profiling);

      if (!profiling?.dataset_id || !profiling.file_path || !profiling.file_format) {
        setLoading(false);
        return;
      }

      sessionStorage.setItem("profilingUpload", JSON.stringify(profiling));

      const result = await apiService.getDatasetProfiling(
        profiling.dataset_id,
        profiling.file_path,
        profiling.file_format,
        domain
      );

      if (result.success) setData(result);

      setLoading(false);

    })();

  }, [searchParams]);

  if (loading) {
    return (
      <AppLayout title="Loading...">
        <div className="py-20 text-center text-muted-foreground">
          Loading dataset...
        </div>
      </AppLayout>
    );
  }

  if (!data) {
    return (
      <AppLayout title="Task Not Found">
        <div className="py-20 text-center">
          {uploadData
            ? "Failed to load dataset."
            : "Dataset details are missing. Reopen this task from the upload flow."}
        </div>
      </AppLayout>
    );
  }

  const { profiling } = data;

  const totalNulls = profiling.columns.reduce(
    (sum, col) => sum + col.null_count,
    0
  );

  const totalCells = profiling.row_count * profiling.column_count;

  const overallNullRate =
    totalCells > 0 ? ((totalNulls / totalCells) * 100).toFixed(2) : "0.00";

  return (
    <AppLayout
      title={`Dataset: ${data.dataset_id}`}
      subtitle="Exploration Task Detail"
    >
      <div className="space-y-6">

        <Button
          variant="ghost"
          size="sm"
          onClick={() => router.push("/misc-tools/data-profiler")}
          className="gap-1"
        >
          <ArrowLeft className="w-4 h-4" /> Back to Data Profiler
        </Button>

        {/* Overview */}

        <CollapsiblePanel title="Overview">

          <div className="space-y-2 text-sm text-muted-foreground leading-relaxed">

            {data.overview?.map((text, i) => (
              <p key={i}>{text}</p>
            ))}

          </div>

        </CollapsiblePanel>

        {/* Details */}

        <CollapsiblePanel title="Details">

          <div className="space-y-4">

            <div className="flex items-center gap-2 text-sm">
              <User className="w-4 h-4 text-muted-foreground" />
              <span className="text-muted-foreground">Created by</span>
              <span className="ml-auto font-medium text-foreground">
                Demo
              </span>
            </div>

            <div className="flex items-center gap-2 text-sm">
              <Calendar className="w-4 h-4 text-muted-foreground" />
              <span className="text-muted-foreground">Created</span>
              <span className="ml-auto font-medium text-foreground">
                {new Date().toLocaleDateString("en-GB")}
              </span>
            </div>

            {/* File */}

            <div className="space-y-2 pt-2">

              <div className="flex items-center justify-between">

                <span className="text-sm text-muted-foreground flex items-center gap-2">
                  <File className="w-4 h-4" /> Files
                </span>

                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs gap-1"
                >
                  <Plus className="w-3 h-3" /> Add File
                </Button>

              </div>

              <div className="flex items-center gap-3 p-2.5 rounded-lg border border-border bg-muted/30">

                <File className="w-4 h-4 text-primary flex-shrink-0" />

                <div className="flex-1 min-w-0">

                  <p className="text-sm font-medium text-foreground truncate">
                    {uploadData?.dataset_id}
                  </p>

                  <p className="text-xs text-muted-foreground">
                    {uploadData?.file_format}
                  </p>

                </div>

              </div>

            </div>

          </div>

        </CollapsiblePanel>

        {/* Tabs */}

        <Tabs defaultValue="schema">

          <TabsList>

            <TabsTrigger value="schema" className="gap-1.5">
              <Database className="w-3.5 h-3.5" /> Schema
            </TabsTrigger>

            <TabsTrigger value="sample-stats" className="gap-1.5">
              <BarChart3 className="w-3.5 h-3.5" /> Sample Stats
            </TabsTrigger>

            <TabsTrigger value="sample-rows" className="gap-1.5">
              <TableIcon className="w-3.5 h-3.5" /> Sample Rows
            </TabsTrigger>

            <TabsTrigger value="deep-exploration" className="gap-1.5">
              <Sparkles className="w-3.5 h-3.5" /> Deep Exploration
            </TabsTrigger>

          </TabsList>

          {/* Schema */}

          <TabsContent value="schema">

            <Card>

              <CardHeader>
                <CardTitle className="text-sm">Detected Schema</CardTitle>
              </CardHeader>

              <CardContent>

                <Table>

                  <TableHeader>

                    <TableRow>
                      <TableHead>Column</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Nullable</TableHead>
                      <TableHead className="text-right">Distinct</TableHead>
                    </TableRow>

                  </TableHeader>

                  <TableBody>

                    {profiling.columns.map((col) => (

                      <TableRow key={col.column_name}>

                        <TableCell className="font-mono text-sm font-medium">
                          {col.column_name}
                        </TableCell>

                        <TableCell>
                          <Badge variant="outline" className="font-mono text-xs">
                            {col.data_type}
                          </Badge>
                        </TableCell>

                        <TableCell className="text-sm text-muted-foreground">
                          {col.nullable ? "Yes" : "No"}
                        </TableCell>

                        <TableCell className="text-right text-sm">
                          {col.distinct_count.toLocaleString()}
                        </TableCell>

                      </TableRow>

                    ))}

                  </TableBody>

                </Table>

              </CardContent>

            </Card>

          </TabsContent>

          {/* Sample Stats */}

          <TabsContent value="sample-stats">

            <Card>

              <CardHeader>
                <CardTitle className="text-sm">Sample Statistics</CardTitle>
              </CardHeader>

              <CardContent>

                <div className="grid grid-cols-2 gap-4 mb-6">

                  <div className="p-4 rounded-lg border">
                    <p className="text-xs text-muted-foreground">Total Rows</p>
                    <p className="text-xl font-bold mt-1">
                      {profiling.row_count}
                    </p>
                  </div>

                  <div className="p-4 rounded-lg border">
                    <p className="text-xs text-muted-foreground">
                      Total Columns
                    </p>
                    <p className="text-xl font-bold mt-1">
                      {profiling.column_count}
                    </p>
                  </div>

                  <div className="p-4 rounded-lg border">
                    <p className="text-xs text-muted-foreground">Null Rate</p>
                    <p className="text-xl font-bold mt-1">
                      {overallNullRate}%
                    </p>
                  </div>

                  <div className="p-4 rounded-lg border">
                    <p className="text-xs text-muted-foreground">
                      Duplicate Rows
                    </p>
                    <p className="text-xl font-bold mt-1">
                      {profiling.duplicate_rows ?? 0}
                    </p>
                  </div>

                </div>

                <div className="w-full overflow-x-auto">

                  <Table className="min-w-max whitespace-nowrap">

                    <TableHeader>

                      <TableRow>
                        <TableHead>Column</TableHead>
                        <TableHead className="text-right">Min</TableHead>
                        <TableHead className="text-right">Max</TableHead>
                        <TableHead className="text-right">Mean</TableHead>
                        <TableHead className="text-right">Null %</TableHead>
                      </TableRow>

                    </TableHeader>

                    <TableBody>

                      {profiling.columns.map((col) => (

                        <TableRow key={col.column_name}>

                          <TableCell className="font-mono text-sm font-medium">
                            {col.column_name}
                          </TableCell>

                          <TableCell className="text-right text-sm">
                            {col.min ?? "—"}
                          </TableCell>

                          <TableCell className="text-right text-sm">
                            {col.max ?? "—"}
                          </TableCell>

                          <TableCell className="text-right text-sm">
                            {col.mean ?? "—"}
                          </TableCell>

                          <TableCell className="text-right text-sm">
                            {col.null_percentage?.toFixed(2)}%
                          </TableCell>

                        </TableRow>

                      ))}

                    </TableBody>

                  </Table>

                </div>

              </CardContent>

            </Card>

          </TabsContent>

          {/* Sample Rows */}

          <TabsContent value="sample-rows">

            <Card className="overflow-hidden">

              <CardHeader>
                <CardTitle className="text-sm">Sample Rows</CardTitle>
              </CardHeader>

              <CardContent className="overflow-hidden">

                <div className="overflow-x-auto max-w-[calc(100vw-25rem)]">

                  <Table className="whitespace-nowrap">

                    <TableHeader>

                      <TableRow>

                        {profiling.columns.map((col) => (
                          <TableHead key={col.column_name}>
                            {col.column_name}
                          </TableHead>
                        ))}

                      </TableRow>

                    </TableHeader>

                    <TableBody>

                      {Array.from({ length: 5 }).map((_, rowIndex) => (

                        <TableRow key={rowIndex}>

                          {profiling.columns.map((col) => (

                            <TableCell key={col.column_name}>
                              {String(col.sample_values?.[rowIndex] ?? "—")}
                            </TableCell>

                          ))}

                        </TableRow>

                      ))}

                    </TableBody>

                  </Table>

                </div>

                <p className="text-xs text-muted-foreground mt-3">
                  Preview based on sample values
                </p>

              </CardContent>

            </Card>

          </TabsContent>

          <TabsContent value="deep-exploration">

            <Card>
              <DeepExplorerInsight dataset_id={data.dataset_id}
                responses={responses} setResponses={setResponses} />
            </Card>

          </TabsContent>

        </Tabs>

      </div>

    </AppLayout>
  );
}
