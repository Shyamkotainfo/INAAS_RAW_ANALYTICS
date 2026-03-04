"use client";

import { AppLayout } from "@/components/layout/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ArrowLeft, Database, BarChart3, ChevronDown, TableIcon, Sparkles } from "lucide-react";
import { useRouter } from "next/navigation";
import { useState, useEffect, ReactNode, useRef } from "react";
import { apiService } from "@/services/apiService";
import DeepExplorerInsight from "@/components/DeepExplorer";

// interface TopValue {
//   value: string | null;
//   count: number;
// }
type TopValue = {
  value: string | number | null;
  count: number;
};
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
  top_values?: TopValue[];
}

interface ProfilingResponse {
  success: boolean;
  dataset_id: string;
  file_path: string;
  profiling: {
    row_count: number;
    column_count: number;
    columns: ColumnProfile[];
  };
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
  // const { id } = useParams<{ id: string }>();
  const router = useRouter();

  const [data, setData] = useState<ProfilingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const fetchedRef = useRef(false);

  useEffect(() => {
    // if (!id) return;

    (async function () {
      if (fetchedRef.current) return;
      fetchedRef.current = true;
      const stored = sessionStorage.getItem("profilingUpload");
      const profiling = stored ? JSON.parse(stored) : null;
      console.log(profiling)
      const result = await apiService.getDatasetProfiling(profiling.dataset_id, profiling.file_path, profiling.file_format);
      if (result.success) setData(result);
      console.log(result)
      setLoading(false);
    })();

    // load();
  }, []);

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
          Failed to load dataset.
        </div>
      </AppLayout>
    );
  }

  const { profiling } = data;

  const totalNulls = profiling.columns.reduce((sum, col) => sum + col.null_count, 0);
  const nullRate = ((totalNulls / (profiling.row_count * profiling.column_count)) * 100).toFixed(2);

  const totalCells = profiling.row_count * profiling.column_count;

  const overallNullRate =
    totalCells > 0 ? ((totalNulls / totalCells) * 100).toFixed(2) : "0.00";
  return (
    <AppLayout title={`Dataset: ${data.dataset_id}`} subtitle="Exploration Task Detail">
      <div className="space-y-6">
        <Button variant="ghost" size="sm" onClick={() => router.push("/misc-tools/data-profiler")} className="gap-1">
          <ArrowLeft className="w-4 h-4" /> Back to Data Profiler
        </Button>

        {/* Overview */}
        <CollapsiblePanel title="Overview">
          <p className="text-sm text-muted-foreground leading-relaxed break-all">
            {data.file_path}
          </p>
        </CollapsiblePanel>

        {/* Details */}
        <CollapsiblePanel title="Details">
          <div className="space-y-4 text-sm">
            <div className="flex justify-between">
              <span className="text-muted-foreground">Rows</span>
              <span className="font-medium">{profiling.row_count.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted-foreground">Columns</span>
              <span className="font-medium">{profiling.column_count}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted-foreground">Null Rate</span>
              <span className="font-medium">{nullRate}%</span>
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
                {/* Summary Grid */}
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
                    <p className="text-xl font-bold mt-1">—</p>
                  </div>
                </div>

                {/* Column Stats */}
                {/* <div className="overflow-x-auto overflow-y-auto max-h-[400px] border rounded-md"> */}
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

          {/* Sample Rows → Using Top Values Preview */}
          <TabsContent value="sample-rows">
            <Card className="overflow-hidden">
              <CardHeader>
                <CardTitle className="text-sm">Sample Rows</CardTitle>
              </CardHeader>

              <CardContent className="overflow-hidden">
                <div className="overflow-x-auto max-w-[calc(100vw-320px)]">
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
                          {profiling.columns.map((col) => {
                            const nonNullValues =
                              col.top_values?.filter(
                                (tv) =>
                                  tv.value !== null &&
                                  tv.value !== "null" &&
                                  String(tv.value).trim() !== ""
                              ) || [];

                            const value = nonNullValues[rowIndex]?.value ?? "—";

                            return (
                              <TableCell key={col.column_name}>
                                {String(value)}
                              </TableCell>
                            );
                          })}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                <p className="text-xs text-muted-foreground mt-3">
                  Preview based on most frequent values
                </p>
              </CardContent>
            </Card>
          </TabsContent>
          <TabsContent value="deep-exploration">
            <Card><DeepExplorerInsight dataset_id={data.dataset_id}/></Card>
          </TabsContent>

        </Tabs>
      </div>
    </AppLayout>
  );
}