"use client";
import { AppLayout } from "@/components/layout/AppLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Plus, MoreHorizontal, Trash2, Share2, Copy, Mail, MessageSquare, Users, Beaker } from "lucide-react";
import { useState } from "react";
// import { useNavigate } from "react-router-dom";
import { useRouter } from "next/navigation";

import { DeleteConfirmDialog } from "@/components/governance/DeleteConfirmDialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSub, DropdownMenuSubContent, DropdownMenuSubTrigger, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { toast } from "sonner";

interface SynthDataset {
  id: string;
  name: string;
  description: string;
  format: string;
  rowCount: number;
  fieldCount: number;
  createdAt: string;
  createdBy: string;
}

const myDatasets: SynthDataset[] = [
  { id: "1", name: "Customer Transactions Mock", description: "Synthetic transaction data with realistic distributions for testing payment pipeline.", format: "CSV", rowCount: 50000, fieldCount: 12, createdAt: "2025-02-10", createdBy: "Current User" },
  { id: "2", name: "IoT Sensor Readings", description: "Simulated temperature and humidity sensor data with normal distribution.", format: "Parquet", rowCount: 1000000, fieldCount: 8, createdAt: "2025-02-08", createdBy: "Current User" },
  { id: "3", name: "User Profiles Test Set", description: "Fake user profile data for QA testing with PII-like fields.", format: "JSON", rowCount: 10000, fieldCount: 15, createdAt: "2025-02-05", createdBy: "Current User" },
  { id: "4", name: "Inventory Stock Levels", description: "Warehouse inventory mock data with skewed distribution on quantities.", format: "CSV", rowCount: 25000, fieldCount: 9, createdAt: "2025-01-28", createdBy: "Current User" },
];

const sharedDatasets: SynthDataset[] = [
  { id: "s1", name: "Healthcare Claims Sample", description: "Synthetic claims data shared by the analytics team for model training.", format: "Parquet", rowCount: 200000, fieldCount: 22, createdAt: "2025-02-12", createdBy: "Jane Smith" },
  { id: "s2", name: "E-commerce Clickstream", description: "Simulated user clickstream data for funnel analysis testing.", format: "JSON", rowCount: 500000, fieldCount: 11, createdAt: "2025-02-06", createdBy: "Mike Chen" },
];

function DatasetTable({ datasets, navigate, setDeleteTarget, isShared }: {
  datasets: SynthDataset[];
  navigate: ReturnType<typeof useRouter>;
  setDeleteTarget: (ds: SynthDataset) => void;
  isShared?: boolean;
}) {
  return (
    <Card>
      <CardContent className="pt-6">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className={isShared ? "w-[40%]" : "w-[50%]"}>Name</TableHead>
              {isShared && <TableHead className="w-[15%]">Shared By</TableHead>}
              <TableHead className="w-[10%]">Format</TableHead>
              <TableHead className="w-[12%]">Rows</TableHead>
              <TableHead className="w-[12%]">Fields</TableHead>
              <TableHead className={isShared ? "w-[13%]" : "w-[16%]"}>Created</TableHead>
              <TableHead className="w-[8%]"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {datasets.length === 0 ? (
              <TableRow>
                <TableCell colSpan={isShared ? 7 : 6} className="text-center text-muted-foreground py-8">
                  {isShared ? "No datasets have been shared with you yet." : "No datasets yet. Create one to get started."}
                </TableCell>
              </TableRow>
            ) : (
              datasets.map((ds) => (
                <TableRow
                  key={ds.id}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => navigate.push(`/misc-tools/data-synthesizer/${ds.id}`)}
                >
                  <TableCell className="align-middle">
                    <div>
                      <p className="font-medium text-foreground">{ds.name}</p>
                      <p className="text-xs text-muted-foreground truncate max-w-[300px]">{ds.description}</p>
                    </div>
                  </TableCell>
                  {isShared && <TableCell className="align-middle text-sm">{ds.createdBy}</TableCell>}
                  <TableCell className="align-middle">
                    <Badge variant="outline" className="text-xs font-mono">{ds.format}</Badge>
                  </TableCell>
                  <TableCell className="align-middle text-sm font-mono">{ds.rowCount.toLocaleString()}</TableCell>
                  <TableCell className="align-middle text-sm">{ds.fieldCount}</TableCell>
                  <TableCell className="align-middle text-sm text-muted-foreground">{ds.createdAt}</TableCell>
                  <TableCell className="align-middle">
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={(e) => e.stopPropagation()}>
                          <MoreHorizontal className="w-4 h-4" />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuSub>
                          <DropdownMenuSubTrigger onClick={(e) => e.stopPropagation()}>
                            <Share2 className="w-4 h-4 mr-2" /> Share
                          </DropdownMenuSubTrigger>
                          <DropdownMenuSubContent>
                            <DropdownMenuItem onClick={(e) => {
                              e.stopPropagation();
                              navigator.clipboard.writeText(`${window.location.origin}/misc-tools/data-synthesizer/${ds.id}`);
                              toast.success("Link copied to clipboard");
                            }}>
                              <Copy className="w-4 h-4 mr-2" /> Copy Link
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={(e) => {
                              e.stopPropagation();
                              window.open(`mailto:?subject=${encodeURIComponent(ds.name)}&body=${encodeURIComponent(`Check out this synthetic dataset: ${window.location.origin}/misc-tools/data-synthesizer/${ds.id}`)}`);
                            }}>
                              <Mail className="w-4 h-4 mr-2" /> Email
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={(e) => { e.stopPropagation(); toast.info("Slack integration coming soon"); }}>
                              <MessageSquare className="w-4 h-4 mr-2" /> Slack
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={(e) => { e.stopPropagation(); toast.info("Teams integration coming soon"); }}>
                              <MessageSquare className="w-4 h-4 mr-2" /> Microsoft Teams
                            </DropdownMenuItem>
                          </DropdownMenuSubContent>
                        </DropdownMenuSub>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          className="text-destructive focus:text-destructive"
                          onClick={(e) => { e.stopPropagation(); setDeleteTarget(ds); }}
                        >
                          <Trash2 className="w-4 h-4 mr-2" /> Delete
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

export default function DataSynthesizer() {
  const [datasets, setDatasets] = useState<SynthDataset[]>(myDatasets);
  const [deleteTarget, setDeleteTarget] = useState<SynthDataset | null>(null);
  const navigate = useRouter();

  return (
    <AppLayout title="Data Synthesizer" subtitle="Generate artificial datasets that mimic real-world statistical characteristics">
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <Button className="gap-2" onClick={() => navigate.push("/misc-tools/data-synthesizer/new")}>
            <Plus className="w-4 h-4" /> New Synthetic Dataset
          </Button>
          <Badge variant="outline" className="gap-1">
            {datasets.length} Datasets
          </Badge>
        </div>

        <Tabs defaultValue="my-data">
          <TabsList>
            <TabsTrigger value="my-data" className="gap-1.5">
              <Beaker className="w-3.5 h-3.5" /> My Datasets
            </TabsTrigger>
            <TabsTrigger value="shared" className="gap-1.5">
              <Users className="w-3.5 h-3.5" /> Shared with me
            </TabsTrigger>
          </TabsList>

          <TabsContent value="my-data">
            <DatasetTable datasets={datasets} navigate={navigate} setDeleteTarget={setDeleteTarget} />
          </TabsContent>

          <TabsContent value="shared">
            <DatasetTable datasets={sharedDatasets} navigate={navigate} setDeleteTarget={setDeleteTarget} isShared />
          </TabsContent>
        </Tabs>

        <DeleteConfirmDialog
          open={!!deleteTarget}
          onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}
          title="Delete Synthetic Dataset"
          description={`Are you sure you want to delete "${deleteTarget?.name}"? This action cannot be undone.`}
          onConfirm={() => { setDatasets((prev) => prev.filter((d) => d.id !== deleteTarget?.id)); setDeleteTarget(null); }}
        />
      </div>
    </AppLayout>
  );
}
