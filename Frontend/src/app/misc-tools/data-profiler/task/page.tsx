"use client";

import { AppLayout } from "@/components/layout/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ArrowLeft, Database, BarChart3, ChevronDown, TableIcon } from "lucide-react";
import { useRouter } from "next/navigation";
import { useState, useEffect, ReactNode } from "react";
import { apiService } from "@/services/apiService";

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

function CollapsiblePanel({
  title,
  defaultOpen = true,
  badge,
  children,
}: CollapsiblePanelProps) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <Card>
        <CollapsibleTrigger asChild>
          <CardHeader className="cursor-pointer hover:bg-muted/50 transition-colors">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <ChevronDown
                  className={`w-4 h-4 transition-transform ${
                    open ? "" : "-rotate-90"
                  }`}
                />
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
  const [data, setData] = useState<ProfilingResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadProfiling = async () => {
      try {
        const stored = localStorage.getItem("dataset");

        if (!stored) {
          setLoading(false);
          return;
        }

        const dataset = JSON.parse(stored);

        const result = await apiService.getDatasetProfiling(dataset);

        if (result?.success) {
          setData(result);
        }
      } catch (error) {
        console.error("Profiling load failed:", error);
      } finally {
        setLoading(false);
      }
    };

    loadProfiling();
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
      <AppLayout title="No Dataset Found">
        <div className="py-20 text-center">
          Please upload a dataset first.
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
    totalCells > 0
      ? ((totalNulls / totalCells) * 100).toFixed(2)
      : "0.00";

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
          <ArrowLeft className="w-4 h-4" />
          Back to Data Profiler
        </Button>

        {/* Overview */}
        <CollapsiblePanel title="Overview">
          <p className="text-sm text-muted-foreground break-all">
            {data.file_path}
          </p>
        </CollapsiblePanel>

        {/* Details */}
        <CollapsiblePanel title="Details">
          <div className="space-y-4 text-sm">
            <div className="flex justify-between">
              <span>Rows</span>
              <span>{profiling.row_count.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span>Columns</span>
              <span>{profiling.column_count}</span>
            </div>
            <div className="flex justify-between">
              <span>Null Rate</span>
              <span>{overallNullRate}%</span>
            </div>
          </div>
        </CollapsiblePanel>

        {/* Tabs */}
        <Tabs defaultValue="schema">
          <TabsList>
            <TabsTrigger value="schema">
              <Database className="w-3.5 h-3.5" /> Schema
            </TabsTrigger>
            <TabsTrigger value="sample-stats">
              <BarChart3 className="w-3.5 h-3.5" /> Sample Stats
            </TabsTrigger>
            <TabsTrigger value="sample-rows">
              <TableIcon className="w-3.5 h-3.5" /> Sample Rows
            </TabsTrigger>
          </TabsList>

          <TabsContent value="schema">
            <Card>
              <CardHeader>
                <CardTitle>Detected Schema</CardTitle>
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
                        <TableCell>{col.column_name}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{col.data_type}</Badge>
                        </TableCell>
                        <TableCell>
                          {col.nullable ? "Yes" : "No"}
                        </TableCell>
                        <TableCell className="text-right">
                          {col.distinct_count.toLocaleString()}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="sample-stats">
            <Card>
              <CardHeader>
                <CardTitle>Column Statistics</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
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
                        <TableCell>{col.column_name}</TableCell>
                        <TableCell className="text-right">
                          {col.min ?? "—"}
                        </TableCell>
                        <TableCell className="text-right">
                          {col.max ?? "—"}
                        </TableCell>
                        <TableCell className="text-right">
                          {col.mean ?? "—"}
                        </TableCell>
                        <TableCell className="text-right">
                          {col.null_percentage?.toFixed(2)}%
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

        </Tabs>
      </div>
    </AppLayout>
  );
}