
"use client";

import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Plus, MoreHorizontal, Trash2, Share2, Copy, Mail, MessageSquare, Users } from "lucide-react";
import { useState } from "react";
import { CreateExplorationTaskDialog } from "@/components/misc/CreateExplorationTaskDialog";
import { DeleteConfirmDialog } from "@/components/governance/DeleteConfirmDialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSub, DropdownMenuSubContent, DropdownMenuSubTrigger, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { toast } from "sonner";
import { useRouter } from "next/navigation";
import { AppLayout } from "@/components/layout/AppLayout";
import { apiService } from "@/services/apiService";

interface ExplorationTask {
  id: string;
  name: string;
  description: string;
  status: string;
  fileName: string;
  fileSize: string;
  createdAt: string;
  createdBy: string;
}

const initialTasks: ExplorationTask[] = [
  { id: "1", name: "Q4 Revenue Analysis", description: "Analyze Q4 revenue trends across product lines and regions.", status: "In Progress", fileName: "revenue_q4_2024.csv", fileSize: "2.4 MB", createdAt: "2024-12-15", createdBy: "John Doe" },
  { id: "2", name: "Customer Churn Prediction", description: "Build a dataset for churn prediction model.", status: "Completed", fileName: "churn_dataset.parquet", fileSize: "18.7 MB", createdAt: "2024-12-10", createdBy: "Jane Smith" },
  { id: "3", name: "Inventory Reconciliation", description: "Cross-reference warehouse inventory with sales records.", status: "Pending", fileName: "inventory_dec.xlsx", fileSize: "5.1 MB", createdAt: "2024-12-18", createdBy: "Mike Chen" },
  { id: "4", name: "Marketing Campaign ROI", description: "Calculate ROI for Q3-Q4 marketing campaigns across channels.", status: "In Progress", fileName: "campaigns_2024.json", fileSize: "1.2 MB", createdAt: "2024-12-12", createdBy: "Sarah Lee" },
  { id: "5", name: "Data Quality Audit", description: "Perform comprehensive data quality check across core tables.", status: "Failed", fileName: "", fileSize: "", createdAt: "2024-12-17", createdBy: "Tom Wilson" },
];

const sharedTasks: ExplorationTask[] = [
  { id: "s1", name: "Regional Sales Breakdown", description: "Breakdown of sales performance by region for Q4 planning.", status: "Completed", fileName: "regional_sales.csv", fileSize: "3.8 MB", createdAt: "2024-12-08", createdBy: "Jane Smith" },
  { id: "s2", name: "User Engagement Metrics", description: "Weekly active users and session duration analysis shared by product team.", status: "In Progress", fileName: "engagement_weekly.parquet", fileSize: "9.2 MB", createdAt: "2024-12-14", createdBy: "Mike Chen" },
  { id: "s3", name: "Supplier Cost Analysis", description: "Comparative supplier pricing data shared by procurement.", status: "Pending", fileName: "supplier_costs.xlsx", fileSize: "1.6 MB", createdAt: "2024-12-16", createdBy: "Sarah Lee" },
];

// const statusConfig: Record<string, { variant: "default" | "secondary" | "destructive" | "outline"; icon: typeof Play }> = {
//   "In Progress": { variant: "default", icon: Play },
//   "Completed": { variant: "secondary", icon: CheckCircle2 },
//   "Pending": { variant: "outline", icon: Clock },
//   "Failed": { variant: "destructive", icon: AlertCircle },
// };

function TaskTable({ tasks, navigate, setDeleteTarget, isShared }: {
  tasks: ExplorationTask[];
  navigate: ReturnType<typeof useRouter>;
  setDeleteTarget: (task: ExplorationTask) => void;
  isShared?: boolean;
}) {
  return (
    <Card>
      <CardContent className="pt-6">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className={isShared ? "w-[50%]" : "w-[60%]"}>Name</TableHead>
              {isShared && <TableHead className="w-[20%]">Shared By</TableHead>}
              <TableHead className={isShared ? "w-[20%]" : "w-[30%]"}>Created</TableHead>
              <TableHead className="w-[10%]"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {tasks.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} className="text-center text-muted-foreground py-8">
                  {isShared ? "No tasks have been shared with you yet." : "No tasks yet. Create one to get started."}
                </TableCell>
              </TableRow>
            ) : (
              tasks.map((task) => (
                <TableRow
                  key={task.id}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => navigate.push(`/misc-tools/data-profiler/task/${task.id}`)}
                >
                  <TableCell className="align-middle">
                    <div>
                      <p className="font-medium text-foreground">{task.name}</p>
                      <p className="text-xs text-muted-foreground truncate max-w-[300px]">{task.description}</p>
                    </div>
                  </TableCell>
                  {isShared && <TableCell className="align-middle text-sm">{task.createdBy}</TableCell>}
                  <TableCell className="align-middle text-sm text-muted-foreground">{task.createdAt}</TableCell>
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
                              navigator.clipboard.writeText(`${window.location.origin}/misc-tools/data-profiler/task/${task.id}`);
                              toast.success("Link copied to clipboard");
                            }}>
                              <Copy className="w-4 h-4 mr-2" /> Copy Link
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={(e) => {
                              e.stopPropagation();
                              window.open(`mailto:?subject=${encodeURIComponent(task.name)}&body=${encodeURIComponent(`Check out this exploration task: ${window.location.origin}/misc-tools/data-explorer/task/${task.id}`)}`);
                            }}>
                              <Mail className="w-4 h-4 mr-2" /> Email
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={(e) => {
                              e.stopPropagation();
                              toast.info("Slack integration coming soon");
                            }}>
                              <MessageSquare className="w-4 h-4 mr-2" /> Slack
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={(e) => {
                              e.stopPropagation();
                              toast.info("Teams integration coming soon");
                            }}>
                              <MessageSquare className="w-4 h-4 mr-2" /> Microsoft Teams
                            </DropdownMenuItem>
                          </DropdownMenuSubContent>
                        </DropdownMenuSub>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          className="text-destructive focus:text-destructive"
                          onClick={(e) => { e.stopPropagation(); setDeleteTarget(task); }}
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

export default function DataExplorer() {
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [tasks, setTasks] = useState<ExplorationTask[]>(initialTasks);
  const [deleteTarget, setDeleteTarget] = useState<ExplorationTask | null>(null);
  const navigate = useRouter();


  // const handleCreateTask = (task: { name: string; description: string; fileName: string; fileSize: string }) => {
  //   const newTask: ExplorationTask = {
  //     id: String(Date.now()),
  //     name: task.name,
  //     description: task.description,
  //     status: "Pending",
  //     fileName: task.fileName,
  //     fileSize: task.fileSize,
  //     createdAt: new Date().toISOString().split("T")[0],
  //     createdBy: "Current User",
  //   };
  //   setTasks((prev) => [newTask, ...prev]);
  // };
  const handleCreateTask = async (task: {
    name: string;
    description: string;
    file?: File; // IMPORTANT: must receive actual File
    file_url?: string
  }) => {
    try {
      const result = await apiService.uploadDataset(task.file, task.file_url);

      // if (!result.success) {
      //   toast.error(result.error || "Upload failed");
      //   return;
      // }

      toast.success("Dataset uploaded successfully");

      // Navigate to task page using returned dataset_id
      sessionStorage.setItem("profilingUpload", JSON.stringify(result));

      navigate.push(
        `/misc-tools/data-profiler/task`
      );
      setCreateDialogOpen(false)

    } catch (error) {
      console.error(error);
      toast.error("Something went wrong");
    }
  };
  return (
    <AppLayout title="Data Profiler" subtitle="Browse, query, and explore your data assets">
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <Button className="gap-2" onClick={() => setCreateDialogOpen(true)}>
            <Plus className="w-4 h-4" /> New Exploration Task
          </Button>
          <Badge variant="outline" className="gap-1">
            {tasks.length} Tasks
          </Badge>
        </div>

        <Tabs defaultValue="my-tasks">
          <TabsList>
            <TabsTrigger value="my-tasks" className="gap-1.5">My Tasks</TabsTrigger>
            <TabsTrigger value="shared" className="gap-1.5"><Users className="w-3.5 h-3.5" /> Shared with me</TabsTrigger>
          </TabsList>

          <TabsContent value="my-tasks">
            <TaskTable tasks={tasks} navigate={navigate} setDeleteTarget={setDeleteTarget} />
          </TabsContent>

          <TabsContent value="shared">
            <TaskTable tasks={sharedTasks} navigate={navigate} setDeleteTarget={setDeleteTarget} isShared />
          </TabsContent>
        </Tabs>

        <CreateExplorationTaskDialog open={createDialogOpen} onOpenChange={setCreateDialogOpen} onSubmit={handleCreateTask} />
        <DeleteConfirmDialog
          open={!!deleteTarget}
          onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}
          title="Delete Exploration Task"
          description={`Are you sure you want to delete "${deleteTarget?.name}"? This action cannot be undone.`}
          onConfirm={() => { setTasks((prev) => prev.filter((t) => t.id !== deleteTarget?.id)); setDeleteTarget(null); }}
        />
      </div>
    </AppLayout>
  );
}