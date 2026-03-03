import { useState, useCallback } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Upload, File, X, Link } from "lucide-react";

interface CreateExplorationTaskDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (task: { name: string; description: string; file?: File, }) => void;
}

export function CreateExplorationTaskDialog({ open, onOpenChange, onSubmit }: CreateExplorationTaskDialogProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [file, setFile] = useState<{ name: string; size: number } | null>(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const [fileUrl, setFileUrl] = useState("");
  const [sourceTab, setSourceTab] = useState("upload");
  const [selectedFile, setSelectedFile] = useState<File | undefined>(undefined);
  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const handleFile = (f: File) => {
    setFile({ name: f.name, size: f.size });
  };

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
    if (e.dataTransfer.files?.[0]) handleFile(e.dataTransfer.files[0]);
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback(() => setIsDragOver(false), []);

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setSelectedFile(e.target.files[0]); // STORE REAL FILE
    }
    if (e.target.files?.[0]) handleFile(e.target.files[0]);

  };

  // const getFileName = () => {
  //   if (sourceTab === "upload" && file) return file.name;
  //   if (sourceTab === "link" && fileUrl.trim()) {
  //     try {
  //       const url = new URL(fileUrl.trim());
  //       const path = url.pathname.split("/").pop();
  //       return path || url.hostname;
  //     } catch {
  //       return fileUrl.trim().split("/").pop() || "Linked file";
  //     }
  //   }
  //   return "";
  // };

  // const getFileSize = () => {
  //   if (sourceTab === "upload" && file) return formatSize(file.size);
  //   if (sourceTab === "link" && fileUrl.trim()) return "Linked";
  //   return "";
  // };

  const handleSubmit = () => {
    if (!name.trim()) return;
    onSubmit({
      name: name.trim(),
      description: description.trim(),
      file: selectedFile,
    });
    setName("");
    setDescription("");
    setFile(null);
    setFileUrl("");
    setSourceTab("upload");
    // onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>New Data Exploration Task</DialogTitle>
          <DialogDescription>Create a task to explore and analyze data assets.</DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="task-name">Name</Label>
            <Input id="task-name" placeholder="e.g. Q4 Revenue Analysis" value={name} onChange={(e) => setName(e.target.value)} />
          </div>

          <div className="space-y-2">
            <Label htmlFor="task-desc">Description</Label>
            <Textarea id="task-desc" placeholder="Describe what you want to explore..." value={description} onChange={(e) => setDescription(e.target.value)} rows={3} />
          </div>

          <div className="space-y-2">
            <Label>Data Source</Label>
            <Tabs value={sourceTab} onValueChange={setSourceTab}>
              <TabsList className="w-full">
                <TabsTrigger value="upload" className="flex-1 gap-1.5"><Upload className="w-3.5 h-3.5" /> Upload File</TabsTrigger>
                <TabsTrigger value="link" className="flex-1 gap-1.5"><Link className="w-3.5 h-3.5" /> File URL</TabsTrigger>
              </TabsList>

              <TabsContent value="upload" className="mt-3">
                <div
                  onDrop={handleDrop}
                  onDragOver={handleDragOver}
                  onDragLeave={handleDragLeave}
                  className={`relative flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed p-6 transition-colors cursor-pointer ${isDragOver ? "border-primary bg-primary/5" : "border-border hover:border-muted-foreground/50"
                    }`}
                  onClick={() => document.getElementById("file-upload")?.click()}
                >
                  <Upload className="w-8 h-8 text-muted-foreground" />
                  <div className="text-center">
                    <p className="text-sm font-medium text-foreground">Drag & drop your file here</p>
                    <p className="text-xs text-muted-foreground mt-1">or click to browse</p>
                  </div>
                  <p className="text-xs text-muted-foreground">CSV, JSON, Parquet, Excel up to 50MB</p>
                  <input id="file-upload" type="file" className="hidden" accept=".csv,.json,.parquet,.xlsx,.xls" onChange={handleFileInput} />
                </div>

                {file && (
                  <div className="flex items-center gap-3 p-3 rounded-lg border border-border bg-muted/30 mt-3">
                    <File className="w-4 h-4 text-primary flex-shrink-0" />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-foreground truncate">{file.name}</p>
                      <p className="text-xs text-muted-foreground">{formatSize(file.size)}</p>
                    </div>
                    <Button variant="ghost" size="icon" className="h-7 w-7" onClick={(e) => { e.stopPropagation(); setFile(null); }}>
                      <X className="w-3 h-3" />
                    </Button>
                  </div>
                )}
              </TabsContent>

              <TabsContent value="link" className="mt-3 space-y-2">
                <Input
                  placeholder="https://example.com/data/report.csv"
                  value={fileUrl}
                  onChange={(e) => setFileUrl(e.target.value)}
                  type="url"
                />
                <p className="text-xs text-muted-foreground">Paste a direct link to a CSV, JSON, Parquet, or Excel file</p>
              </TabsContent>
            </Tabs>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={handleSubmit} disabled={!name.trim()}>Create Task</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
