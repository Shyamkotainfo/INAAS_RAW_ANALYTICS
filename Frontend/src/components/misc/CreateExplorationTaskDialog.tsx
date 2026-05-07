import { useState, useCallback, useRef, useEffect } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
// import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Upload, File, X, Link } from "lucide-react";
import { Progress } from "../ui/progress";

interface CreateExplorationTaskDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (task: { name: string; domain: string; file?: File, }) => void;
}

export function CreateExplorationTaskDialog({ open, onOpenChange, onSubmit }: CreateExplorationTaskDialogProps) {
  const [name, setName] = useState("");
  const [domain, setDomain] = useState("");
  const [file, setFile] = useState<{ name: string; size: number } | null>(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const [fileUrl, setFileUrl] = useState("");
  const [sourceTab, setSourceTab] = useState("upload");
  const [selectedFile, setSelectedFile] = useState<File | undefined>(undefined);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [isUploading, setIsUploading] = useState(false);
  const uploadIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  useEffect(() => {
    return () => {
      if (uploadIntervalRef.current) clearInterval(uploadIntervalRef.current);
    };
  }, []);
  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };
  const simulateUpload = (f: File) => {
    setIsUploading(true);
    setUploadProgress(0);
    // Simulate progress based on file size (larger = slower)
    const duration = Math.min(Math.max(f.size / 50000, 1000), 15000);
    const steps = 50;
    const increment = 100 / steps;
    const interval = duration / steps;
    let current = 0;
    uploadIntervalRef.current = setInterval(() => {
      current += increment;
      if (current >= 100) {
        setUploadProgress(100);
        setIsUploading(false);
        setFile({ name: f.name, size: f.size });
        if (uploadIntervalRef.current) clearInterval(uploadIntervalRef.current);
      } else {
        setUploadProgress(Math.round(current));
      }
    }, interval);
  };

  const handleFile = (f: File) => {
    simulateUpload(f);
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
    if (!domain.trim()) return;

    onSubmit({
      name: name.trim(),
      domain: domain.trim(),
      ...file && { file: selectedFile },
      ...fileUrl && { file_url: fileUrl }
    });
    setName("");
    setDomain("");
    setFile(null);
    setFileUrl("");
    setSourceTab("upload");
    // onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>New Data Exploration Task</DialogTitle>
          <DialogDescription>Create a task to explore and analyze data assets.</DialogDescription>
        </DialogHeader>

        <div className="space-y-4 overflow-y-auto py-2 px-1">
          <div className="space-y-2">
            <Label htmlFor="task-name">Name</Label>
            <Input id="task-name" placeholder="e.g. Q4 Revenue Analysis" value={name} onChange={(e) => setName(e.target.value)} />
          </div>

          <div className="space-y-2">
            <Label htmlFor="task-desc">Domain</Label>
            {/* <Textarea id="task-desc" placeholder="Describe what you want to explore..." value={domain} onChange={(e) => setDomain(e.target.value)} rows={3} /> */}
            <select
              id="task-desc"
              value={domain}
              onChange={(e) => setDomain(e.target.value)}
              className="w-full border rounded-md p-2"
            >
              <option value="">Select option</option>
              <option value="raw_data">None</option>

              <option value="hr">HR</option>
            </select>
          </div>

          <div className="space-y-2">
            <Label>Data Source</Label>
            <Tabs value={sourceTab} onValueChange={(val) => {
              setSourceTab(val);
              setFile(null);
              setFileUrl("");
              // setSelectedFile()
            }}>
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
                {isUploading && (
                  <div className="mt-3 space-y-2 p-3 rounded-lg border border-border bg-muted/30">
                    <div className="flex items-center gap-2">
                      <div className="h-2 w-2 rounded-full bg-primary animate-pulse" />
                      <p className="text-sm font-medium text-foreground">Uploading file…</p>
                      <span className="ml-auto text-xs text-muted-foreground">{uploadProgress}%</span>
                    </div>
                    <Progress value={uploadProgress} className="h-2" />
                    <p className="text-xs text-muted-foreground animate-pulse">Please wait, large files may take a few minutes</p>
                  </div>
                )}

                { file && (
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
          <Button onClick={handleSubmit} disabled={!name.trim() || !domain.trim()}>Create Task</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
