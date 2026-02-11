import { LucideIcon, ArrowRight, Lock } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import Link from "next/link";

interface ProductCardProps {
  name: string;
  description: string;
  icon: LucideIcon;
  path: string;
  color: string;
  stats: {
    label: string;
    value: string;
    status?: "success" | "warning" | "error";
  };
  locked?: boolean;
}

export function ProductCard({ name, description, icon: Icon, path, color, stats, locked }: ProductCardProps) {
  const CardContent = (
    <div className={cn(
      "product-card group block",
      locked && "opacity-60 cursor-not-allowed"
    )}>
      <div className="flex items-start justify-between mb-4">
        <div className={cn("w-12 h-12 rounded-xl flex items-center justify-center", color)}>
          <Icon className="w-6 h-6" />
        </div>
        {locked ? (
          <Lock className="w-5 h-5 text-muted-foreground" />
        ) : (
          <ArrowRight className="w-5 h-5 text-muted-foreground opacity-0 -translate-x-2 group-hover:opacity-100 group-hover:translate-x-0 transition-all duration-200" />
        )}
      </div>
      
      <h3 className="text-lg font-semibold text-foreground mb-1">{name}</h3>
      <p className="text-sm text-muted-foreground mb-4">{description}</p>
      
      <div className="pt-4 border-t border-border/50">
        <div className="flex items-center justify-between">
          <span className="text-sm text-muted-foreground">{stats.label}</span>
          <span className={cn(
            "text-sm font-medium",
            stats.status === "success" && "text-success",
            stats.status === "warning" && "text-warning",
            stats.status === "error" && "text-destructive",
            !stats.status && "text-foreground"
          )}>
            {stats.value}
          </span>
        </div>
      </div>
    </div>
  );

  if (locked) {
    return (
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <div>{CardContent}</div>
          </TooltipTrigger>
          <TooltipContent>
            <p>Access restricted</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    );
  }

  return (
    <Link href={path} className="block">
      {CardContent}
    </Link>
  );
}
