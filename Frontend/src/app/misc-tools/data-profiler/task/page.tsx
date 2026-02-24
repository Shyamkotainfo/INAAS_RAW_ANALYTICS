"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

import { ArrowLeft, Database, BarChart3 } from "lucide-react";
import { AppLayout } from "@/components/layout/AppLayout";

interface TopValue {
  value: string | null;
  count: number;
}

interface ColumnProfile {
  column_name: string;
  data_type: string;
  nullable: boolean;
  null_count: number;
  distinct_count: number;
  top_values: TopValue[];
}

interface ProfilingData {
  row_count: number;
  column_count: number;
  columns: ColumnProfile[];
}

export default function ExplorationTaskDetail() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();

  const [profiling, setProfiling] = useState<ProfilingData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // if (!id) return;

    async function fetchProfiling() {
      try {
        setLoading(true);

        const res = await fetch(
          `https://2diicvb4i6.us-east-1.awsapprunner.com/profiling`,
          { cache: "no-store" }
        );
        console.log(res,'json')

        const json = await res.json();
        console.log(json,'json')

        // if (!json.success) {
        //   throw new Error("Failed to fetch profiling data");
        // }
        console.log(json,'json')
        setProfiling(json.profiling);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }

    fetchProfiling();
  }, [id]);

  if (loading) {
    return (
      <AppLayout title="Loading Dataset">
        <div className="py-20 text-center text-muted-foreground">
          Loading profiling results...
        </div>
      </AppLayout>
    );
  }

  if (error || !profiling) {
    return (
      <AppLayout title="Dataset Not Found">
        <div className="py-20 text-center text-muted-foreground space-y-4">
          <p>Failed to load dataset.</p>
          <Button
            variant="outline"
            onClick={() => router.push("/misc-tools/data-profiler")}
          >
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Data Profiler
          </Button>
        </div>
      </AppLayout>
    );
  }

  const totalNulls = profiling.columns.reduce(
    (sum, col) => sum + col.null_count,
    0
  );

  const nullRate = (
    (totalNulls / (profiling.row_count * profiling.column_count)) *
    100
  ).toFixed(2);

  return (
    <AppLayout
      title={`Dataset: ${id}`}
      subtitle="Data Profiling Result"
    >
      <div className="space-y-6">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => router.push("/misc-tools/data-profiler")}
          className="gap-1"
        >
          <ArrowLeft className="w-4 h-4" />
          Back
        </Button>

        <Tabs defaultValue="schema">
          <TabsList>
            <TabsTrigger value="schema" className="gap-1.5">
              <Database className="w-3.5 h-3.5" />
              Schema
            </TabsTrigger>
            <TabsTrigger value="stats" className="gap-1.5">
              <BarChart3 className="w-3.5 h-3.5" />
              Statistics
            </TabsTrigger>
          </TabsList>

          {/* ================= SCHEMA TAB ================= */}
          <TabsContent value="schema">
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">
                  Detected Schema ({profiling.column_count} columns)
                </CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Column</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Nullable</TableHead>
                      <TableHead className="text-right">
                        Distinct Values
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {profiling.columns.map((col) => (
                      <TableRow key={col.column_name}>
                        <TableCell className="font-mono text-sm font-medium">
                          {col.column_name}
                        </TableCell>
                        <TableCell>
                          <Badge
                            variant="outline"
                            className="font-mono text-xs"
                          >
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

          {/* ================= STATS TAB ================= */}
          <TabsContent value="stats">
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">
                  Dataset Statistics
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4">
                  <div className="p-4 rounded-lg border">
                    <p className="text-xs text-muted-foreground">
                      Total Rows
                    </p>
                    <p className="text-xl font-bold mt-1">
                      {profiling.row_count.toLocaleString()}
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
                    <p className="text-xs text-muted-foreground">
                      Total Null Values
                    </p>
                    <p className="text-xl font-bold mt-1">
                      {totalNulls.toLocaleString()}
                    </p>
                  </div>

                  <div className="p-4 rounded-lg border">
                    <p className="text-xs text-muted-foreground">
                      Null Rate
                    </p>
                    <p className="text-xl font-bold mt-1">
                      {nullRate}%
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </AppLayout>
  );
}