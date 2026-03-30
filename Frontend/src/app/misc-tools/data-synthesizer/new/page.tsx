"use client";
import { useState } from "react";
import { AppLayout } from "@/components/layout/AppLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { toast } from "@/hooks/use-toast";
import {
  Beaker,
  Plus,
  Trash2,
  Play,
  Download,
  Sparkles,
  FileJson,
  FileSpreadsheet,
  FileText,
  Database,
  Copy,
  Settings2,
  FolderOpen,
  Upload,
  X,
  FileUp,
  ChevronDown,
  Layers,
  // History,
  // Eye,
} from "lucide-react";
import { apiService } from "@/services/apiService";

type FieldType = "string" | "integer" | "float" | "boolean" | "date" | "datetime" | "email" | "uuid" | "enum" | "phone" | "address" | "name";
type Distribution = "uniform" | "normal" | "skewed_left" | "skewed_right" | "exponential" | "custom";

interface SynthField {
  id: string;
  name: string;
  type: FieldType;
  distribution: Distribution;
  nullable: boolean;
  nullPercent: number;
  minValue: string;
  maxValue: string;
  enumValues: string;
  pattern: string;
}

const defaultField = (): SynthField => ({
  id: crypto.randomUUID(),
  name: "",
  type: "string",
  distribution: "uniform",
  nullable: false,
  nullPercent: 5,
  minValue: "",
  maxValue: "",
  enumValues: "",
  pattern: "",
});

const fieldTypeOptions: { value: FieldType; label: string }[] = [
  { value: "string", label: "String" },
  { value: "integer", label: "Integer" },
  { value: "float", label: "Float" },
  { value: "boolean", label: "Boolean" },
  { value: "date", label: "Date" },
  { value: "datetime", label: "Datetime" },
  { value: "email", label: "Email" },
  { value: "uuid", label: "UUID" },
  { value: "enum", label: "Enum" },
  { value: "phone", label: "Phone" },
  { value: "address", label: "Address" },
  { value: "name", label: "Name" },
];

const distributionOptions: { value: Distribution; label: string }[] = [
  { value: "uniform", label: "Uniform" },
  { value: "normal", label: "Normal (Gaussian)" },
  { value: "skewed_left", label: "Skewed Left" },
  { value: "skewed_right", label: "Skewed Right" },
  { value: "exponential", label: "Exponential" },
  { value: "custom", label: "Custom" },
];

const fileFormats = [
  { value: "csv", label: "CSV", icon: FileSpreadsheet },
  { value: "json", label: "JSON", icon: FileJson },
  { value: "parquet", label: "Parquet", icon: Database },
  { value: "tsv", label: "TSV", icon: FileText },
];

interface HistoryEntry {
  id: string;
  rowCount: number;
  location: string;
  format: string;
  createdAt: string;
  schema: { name: string; type: string; distribution: string }[];
}

const initialHistory: HistoryEntry[] = [
  { id: "h1", rowCount: 50000, location: "s3://data-lake/synth/customers_v3.csv", format: "CSV", createdAt: "2025-02-14 14:32", schema: [{ name: "id", type: "UUID", distribution: "Uniform" }, { name: "email", type: "Email", distribution: "Uniform" }, { name: "amount", type: "Float", distribution: "Normal" }, { name: "status", type: "Enum", distribution: "Uniform" }] },
  { id: "h2", rowCount: 25000, location: "s3://data-lake/synth/customers_v2.csv", format: "CSV", createdAt: "2025-02-10 09:15", schema: [{ name: "id", type: "UUID", distribution: "Uniform" }, { name: "name", type: "Name", distribution: "Uniform" }, { name: "revenue", type: "Float", distribution: "Skewed Right" }] },
  { id: "h3", rowCount: 100000, location: "gs://analytics/synth/transactions.parquet", format: "Parquet", createdAt: "2025-02-05 17:48", schema: [{ name: "txn_id", type: "UUID", distribution: "Uniform" }, { name: "amount", type: "Float", distribution: "Exponential" }, { name: "ts", type: "Datetime", distribution: "Uniform" }, { name: "category", type: "Enum", distribution: "Uniform" }, { name: "region", type: "String", distribution: "Uniform" }] },
  { id: "h4", rowCount: 10000, location: "/data/output/users_test.json", format: "JSON", createdAt: "2025-01-28 11:20", schema: [{ name: "user_id", type: "Integer", distribution: "Uniform" }, { name: "email", type: "Email", distribution: "Uniform" }] },
];

export default function DataSynthesizer() {
  const [datasetName, setDatasetName] = useState("");
  const [description, setDescription] = useState("");
  const [aiCriteria, setAiCriteria] = useState("");
  const [rowCount, setRowCount] = useState("10000");
  const [fileFormat, setFileFormat] = useState("csv");
  const [targetLocation, setTargetLocation] = useState("");
  const [schemaFile, setSchemaFile] = useState<{ name: string; size: string } | null>(null);
  const [fields, setFields] = useState<SynthField[]>([defaultField()]);
  const [generating, setGenerating] = useState(false);
  const [generated, setGenerated] = useState(false);
  const [analysed, setAnalysed] = useState(false);
  const [analysing, setAnalysing] = useState(false);

  // const [history, setHistory] = useState<HistoryEntry[]>(initialHistory);
  const [viewSchemaEntry, setViewSchemaEntry] = useState<HistoryEntry | null>(null);
  const [downloadLink, setDownloadLink] = useState<string>("");
  const handleSchemaUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const validExts = [".json", ".yaml", ".yml", ".avsc", ".xsd", ".csv"];
    const ext = file.name.substring(file.name.lastIndexOf(".")).toLowerCase();
    if (!validExts.includes(ext)) {
      toast({ title: "Unsupported file type", description: `Supported: ${validExts.join(", ")}`, variant: "destructive" });
      return;
    }
    const size = file.size < 1024 ? `${file.size} B` : file.size < 1048576 ? `${(file.size / 1024).toFixed(1)} KB` : `${(file.size / 1048576).toFixed(1)} MB`;
    setSchemaFile({ name: file.name, size });
    toast({ title: "Schema file uploaded", description: `${file.name} loaded. Fields will be auto-populated.` });
    // Mock: add sample fields from schema
    setFields([
      { ...defaultField(), name: "id", type: "uuid" },
      { ...defaultField(), name: "created_at", type: "datetime" },
      { ...defaultField(), name: "status", type: "enum", enumValues: "active, inactive, pending" },
      { ...defaultField(), name: "amount", type: "float", distribution: "normal" },
    ]);
  };

  const removeSchemaFile = () => {
    setSchemaFile(null);
    setFields([defaultField()]);
  };

  const addField = () => setFields((prev) => [...prev, defaultField()]);

  const removeField = (id: string) => {
    if (fields.length <= 1) return;
    setFields((prev) => prev.filter((f) => f.id !== id));
  };
  const handleDownload = () => {
    if (!downloadLink) return;

    const link = document.createElement("a");
    link.href = downloadLink;
    link.download = ""; // optional: you can set filename here
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    toast({ title: "Download started" });
  };

  const updateField = (id: string, updates: Partial<SynthField>) => {
    setFields((prev) => prev.map((f) => (f.id === id ? { ...f, ...updates } : f)));
  };
  const handleGenerate = async () => {
    if (!datasetName.trim()) {
      toast({
        title: "Dataset name required",
        variant: "destructive"
      });
      return;
    }

    if (fields.some(f => !f.name.trim())) {
      toast({
        title: "All fields must have a name",
        variant: "destructive"
      });
      return;
    }

    const payload = {
      columns: fields.map(f => ({
        name: f.name,
        nullable: f.nullable,
        type: f.type  // Maps directly: "string", "integer", "date", etc. from FieldType
      })),
      dataset_name: datasetName,
      format: fileFormat,
      primary_key: fields[0]?.name || "id",
      rows: rowCount
    };

    setGenerating(true);

    try {
      // Your existing API call - replace 'YOUR_API_ENDPOINT' with actual endpoint if different
      console.log(payload)

      const response = await apiService.postDataGenerate(payload);
      console.log(payload)
      // if (!response.ok) throw new Error('Generation failed');

      setGenerated(true);
      toast({
        title: "Dataset generated successfully!",
        description: `${rowCount.toLocaleString()} rows created in ${fileFormat.toUpperCase()} format.`,
      });


      setDownloadLink(response?.s3_url); // Assuming API returns a download URL in the response

    } catch (error) {
      toast({
        title: "Generation failed",
        description: "Message error",
        variant: "destructive"
      });
    } finally {
      setGenerating(false);
    }
  };
  const handleAnalyse = async () => {
    if (!datasetName.trim()) {
      toast({
        title: "Dataset name required",
        variant: "destructive"
      });
      return;
    }

    if (fields.some(f => !f.name.trim())) {
      toast({
        title: "All fields must have a name",
        variant: "destructive"
      });
      return;
    }

    const payload = {
      columns: fields.map(f => ({
        name: f.name,
        nullable: f.nullable,
        type: f.type  // Maps directly: "string", "integer", "date", etc. from FieldType
      })),
      dataset_name: datasetName,
      format: fileFormat,
      primary_key: fields[0]?.name || "id",
      rows: rowCount
    };

    setAnalysing(true);

    try {
      // Your existing API call - replace 'YOUR_API_ENDPOINT' with actual endpoint if different
      console.log(payload)

      const response = await apiService.postDataAnalysis(payload)
      // if (!response.ok) throw new Error('Generation failed');

      setAnalysed(true);
      toast({
        title: "Dataset generated successfully!",
        description: `${rowCount.toLocaleString()} rows created in ${fileFormat.toUpperCase()} format.`,
      });


      /* The above code is updating the history state by adding a new entry at the beginning of the
      history array. The new entry includes an id generated using `crypto.randomUUID()`, a rowCount
      converted to a number, a location based on the targetLocation or a default value of
      `.`, the file format, the current date and time in a string format,
      and an array of schema objects derived from the fields array. The code also ensures that the
      history array contains a maximum of 10 entries by slicing the array to keep only the first 10
      elements. */
      // setHistory(prev => [{
      //   id: crypto.randomUUID(),
      //   rowCount: Number(rowCount),  // Convert to number to match HistoryEntry type
      //   location: targetLocation || `${datasetName}.${fileFormat}`,
      //   format: fileFormat,  // Already string, matches HistoryEntry
      //   createdAt: new Date().toLocaleString(),  // String format like existing history
      //   schema: fields.map(f => ({
      //     name: f.name,
      //     type: f.type,
      //     distribution: f.distribution
      //   }))
      // }, ...prev.slice(0, 9)]);

    } catch (error) {
      toast({
        title: "Generation failed",
        description: "Message error",
        variant: "destructive"
      });
    } finally {
      setAnalysing(false);
    }
  };

  return (
    <AppLayout title="Data Synthesizer">
      <div className="p-6 space-y-6 max-w-6xl">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-primary/10">
              <Beaker className="w-6 h-6 text-primary" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-foreground">Data Synthesizer</h1>
              <p className="text-sm text-muted-foreground">
                Generate artificial datasets that mimic real-world statistical characteristics
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {generated && (

              <Button variant="outline" size="sm" onClick={handleDownload}>
                <Download className="w-4 h-4 mr-2" />
                Download
              </Button>)}
            {analysed && (
              <Button variant="outline" size="sm" onClick={handleGenerate}>
                {generating ? (
                  <>
                    <Sparkles className="w-4 h-4 mr-2 animate-spin" />
                    Generating...
                  </>
                ) : (
                  <>
                    <Play className="w-4 h-4 mr-2" />
                    Generate Dataset
                  </>
                )}
              </Button>

            )}
            <Button onClick={handleAnalyse} disabled={analysing} size="sm">
              {analysing ? (
                <>
                  <Sparkles className="w-4 h-4 mr-2 animate-spin" />
                  Analysing...
                </>
              ) : (
                <>
                  <Play className="w-4 h-4 mr-2" />
                  Analyse Data
                </>
              )}
            </Button>
          </div>
        </div>

        {/* Dataset Settings - Collapsible */}
        <Collapsible defaultOpen>
          <Card>
            <CollapsibleTrigger asChild>
              <CardHeader className="cursor-pointer hover:bg-muted/30 transition-colors">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <Settings2 className="w-4 h-4" />
                    Dataset Settings
                  </CardTitle>
                  <ChevronDown className="w-4 h-4 text-muted-foreground transition-transform duration-200 [[data-state=closed]_&]:rotate-[-90deg]" />
                </div>
              </CardHeader>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <CardContent className="space-y-4 pt-0">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="dataset-name">Dataset Name</Label>
                    <Input
                      id="dataset-name"
                      placeholder="e.g. customer_transactions"
                      value={datasetName}
                      onChange={(e) => setDatasetName(e.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="row-count">Number of Rows</Label>
                    <Input
                      id="row-count"
                      type="number"
                      min={1}
                      max={10000000}
                      value={rowCount}
                      onChange={(e) => setRowCount(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">Max: 10,000,000 rows</p>
                  </div>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="description">Description</Label>
                  <Textarea
                    id="description"
                    placeholder="Describe what this dataset represents and its intended use..."
                    rows={3}
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="ai-criteria" className="flex items-center gap-1.5">
                    <Sparkles className="w-3.5 h-3.5 text-primary" />
                    AI Criteria
                  </Label>
                  <Textarea
                    id="ai-criteria"
                    placeholder="Specify correlations, constraints, and statistical properties the AI should enforce (e.g. 'revenue should correlate positively with account age', '30% of records should have null phone numbers')..."
                    rows={3}
                    value={aiCriteria}
                    onChange={(e) => setAiCriteria(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">AI will use these criteria to shape the generated data distributions and relationships</p>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Output Format</Label>
                    <div className="grid grid-cols-2 gap-2">
                      {fileFormats.map((fmt) => (
                        <button
                          key={fmt.value}
                          onClick={() => setFileFormat(fmt.value)}
                          className={`flex items-center gap-2 px-3 py-2 rounded-md border text-sm transition-colors ${fileFormat === fmt.value
                            ? "border-primary bg-primary/10 text-primary font-medium"
                            : "border-border text-muted-foreground hover:border-primary/50"
                            }`}
                        >
                          <fmt.icon className="w-4 h-4" />
                          {fmt.label}
                        </button>
                      ))}
                    </div>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="target-location" className="flex items-center gap-1.5">
                      <FolderOpen className="w-3.5 h-3.5" />
                      Target File Location
                    </Label>
                    <Input
                      id="target-location"
                      placeholder="e.g. s3://bucket/datasets/ or /data/output/"
                      value={targetLocation}
                      onChange={(e) => setTargetLocation(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">S3, GCS, ADLS, or local path</p>
                  </div>
                </div>
              </CardContent>
            </CollapsibleContent>
          </Card>
        </Collapsible>

        {/* Schema Definition - Collapsible */}
        <Collapsible defaultOpen>
          <Card>
            <CollapsibleTrigger asChild>
              <CardHeader className="cursor-pointer hover:bg-muted/30 transition-colors">
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="text-sm flex items-center gap-2">
                      <Layers className="w-4 h-4" />
                      Schema Definition
                    </CardTitle>
                    <CardDescription className="mt-1">Define columns, types, and distribution rules for each field</CardDescription>
                  </div>
                  <ChevronDown className="w-4 h-4 text-muted-foreground transition-transform duration-200 [[data-state=closed]_&]:rotate-[-90deg]" />
                </div>
              </CardHeader>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <CardContent className="space-y-4 pt-0">
                {/* Upload Schema File */}
                <div className="space-y-2">
                  <Label className="flex items-center gap-1.5 text-xs font-medium">
                    <FileUp className="w-3.5 h-3.5" />
                    Upload Schema File
                  </Label>
                  {schemaFile ? (
                    <div className="flex items-center gap-2 p-2.5 rounded-md border border-border bg-muted/30">
                      <FileJson className="w-4 h-4 text-primary flex-shrink-0" />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium truncate">{schemaFile.name}</p>
                        <p className="text-xs text-muted-foreground">{schemaFile.size}</p>
                      </div>
                      <Button variant="ghost" size="icon" className="h-7 w-7 flex-shrink-0" onClick={removeSchemaFile}>
                        <X className="w-3.5 h-3.5" />
                      </Button>
                    </div>
                  ) : (
                    <label className="flex flex-col items-center gap-2 p-4 rounded-md border border-dashed border-border hover:border-primary/50 cursor-pointer transition-colors">
                      <Upload className="w-5 h-5 text-muted-foreground" />
                      <span className="text-xs text-muted-foreground text-center">
                        Drop a schema file or click to browse
                      </span>
                      <span className="text-[10px] text-muted-foreground">
                        JSON, YAML, Avro, XSD, CSV
                      </span>
                      <input type="file" className="hidden" accept=".json,.yaml,.yml,.avsc,.xsd,.csv" onChange={handleSchemaUpload} />
                    </label>
                  )}
                  <p className="text-xs text-muted-foreground">Auto-populates fields from your schema definition</p>
                </div>

                <Separator />

                {/* Fields */}
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-muted-foreground">{fields.length} field{fields.length !== 1 ? "s" : ""} defined</span>
                  <Button variant="outline" size="sm" onClick={addField}>
                    <Plus className="w-4 h-4 mr-1" />
                    Add Field
                  </Button>
                </div>
                <div className="space-y-4">
                  {fields.map((field, idx) => (
                    <div key={field.id} className="border border-border rounded-lg p-4 space-y-3">
                      <div className="flex items-center justify-between">
                        <Badge variant="secondary" className="text-xs">
                          Field {idx + 1}
                        </Badge>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7"
                          onClick={() => removeField(field.id)}
                          disabled={fields.length <= 1}
                        >
                          <Trash2 className="w-3.5 h-3.5 text-muted-foreground" />
                        </Button>
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                        <div className="space-y-1">
                          <Label className="text-xs">Name</Label>
                          <Input
                            placeholder="field_name"
                            value={field.name}
                            onChange={(e) => updateField(field.id, { name: e.target.value })}
                            className="h-8 text-sm"
                          />
                        </div>
                        <div className="space-y-1">
                          <Label className="text-xs">Type</Label>
                          <Select value={field.type} onValueChange={(v) => updateField(field.id, { type: v as FieldType })}>
                            <SelectTrigger className="h-8 text-sm">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {fieldTypeOptions.map((t) => (
                                <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-1">
                          <Label className="text-xs">Distribution</Label>
                          <Select value={field.distribution} onValueChange={(v) => updateField(field.id, { distribution: v as Distribution })}>
                            <SelectTrigger className="h-8 text-sm">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {distributionOptions.map((d) => (
                                <SelectItem key={d.value} value={d.value}>{d.label}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="flex items-end gap-3">
                          <div className="flex items-center gap-2">
                            <Switch
                              checked={field.nullable}
                              onCheckedChange={(v) => updateField(field.id, { nullable: v })}
                            />
                            <Label className="text-xs">Nullable</Label>
                          </div>
                        </div>
                      </div>

                      {/* Conditional config rows */}
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                        {(field.type === "integer" || field.type === "float") && (
                          <>
                            <div className="space-y-1">
                              <Label className="text-xs">Min Value</Label>
                              <Input
                                placeholder="0"
                                value={field.minValue}
                                onChange={(e) => updateField(field.id, { minValue: e.target.value })}
                                className="h-8 text-sm"
                              />
                            </div>
                            <div className="space-y-1">
                              <Label className="text-xs">Max Value</Label>
                              <Input
                                placeholder="100"
                                value={field.maxValue}
                                onChange={(e) => updateField(field.id, { maxValue: e.target.value })}
                                className="h-8 text-sm"
                              />
                            </div>
                          </>
                        )}
                        {field.type === "enum" && (
                          <div className="col-span-2 space-y-1">
                            <Label className="text-xs">Enum Values (comma-separated)</Label>
                            <Input
                              placeholder="active, inactive, pending"
                              value={field.enumValues}
                              onChange={(e) => updateField(field.id, { enumValues: e.target.value })}
                              className="h-8 text-sm"
                            />
                          </div>
                        )}
                        {field.type === "string" && (
                          <div className="col-span-2 space-y-1">
                            <Label className="text-xs">Pattern (regex, optional)</Label>
                            <Input
                              placeholder="e.g. [A-Z]{3}-[0-9]{4}"
                              value={field.pattern}
                              onChange={(e) => updateField(field.id, { pattern: e.target.value })}
                              className="h-8 text-sm"
                            />
                          </div>
                        )}
                        {field.nullable && (
                          <div className="space-y-1">
                            <Label className="text-xs">Null %</Label>
                            <Input
                              type="number"
                              min={0}
                              max={100}
                              value={field.nullPercent}
                              onChange={(e) => updateField(field.id, { nullPercent: Number(e.target.value) })}
                              className="h-8 text-sm"
                            />
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </CollapsibleContent>
          </Card>
        </Collapsible>

        {/* Preview Section - Collapsible */}



        {/* Schema View Dialog */}
        <Dialog open={!!viewSchemaEntry} onOpenChange={(open) => { if (!open) setViewSchemaEntry(null); }}>
          <DialogContent className="max-w-lg">
            <DialogHeader>
              <DialogTitle className="text-sm">Schema — {viewSchemaEntry?.createdAt}</DialogTitle>
            </DialogHeader>
            {viewSchemaEntry && (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-xs">Field</TableHead>
                    <TableHead className="text-xs">Type</TableHead>
                    <TableHead className="text-xs">Distribution</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {viewSchemaEntry.schema.map((s, i) => (
                    <TableRow key={i}>
                      <TableCell className="font-mono text-sm">{s.name}</TableCell>
                      <TableCell>
                        <Badge variant="secondary" className="text-xs">{s.type}</Badge>
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground">{s.distribution}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </AppLayout>
  );
}


