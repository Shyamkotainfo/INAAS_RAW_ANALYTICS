import { useState } from "react";
import { ChevronLeft, ChevronDown, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { usePathname } from "next/navigation";
import Link from "next/link";

interface SubmenuItem {
  name: string;
  path: string;
  hasSubmenu?: boolean;
}

interface SidebarSubmenuProps {
  title: string;
  items: SubmenuItem[];
  nestedSubmenus?: Record<string, SubmenuItem[]>;
  onClose: () => void;
}

export function SidebarSubmenu({ title, items, nestedSubmenus = {}, onClose }: SidebarSubmenuProps) {
  const location = usePathname();
  const [expandedItems, setExpandedItems] = useState<string[]>([]);

  const isActive = (path: string) => {
    const currentPath = location + location.search;
    return currentPath === path;
  };

  const toggleExpanded = (path: string) => {
    setExpandedItems(prev => 
      prev.includes(path) 
        ? prev.filter(p => p !== path)
        : [...prev, path]
    );
  };

  const renderItem = (item: SubmenuItem) => {
    const hasNested = item.hasSubmenu && nestedSubmenus[item.path];
    const isExpanded = expandedItems.includes(item.path);
    const nestedItems = hasNested ? nestedSubmenus[item.path] : [];

    if (hasNested) {
      return (
        <div key={item.path}>
          <button
            onClick={() => toggleExpanded(item.path)}
            className={cn(
              "flex items-center w-full px-3 py-2 text-sm rounded-md transition-colors",
              "text-muted-foreground hover:text-foreground hover:bg-sidebar-accent",
              isActive(item.path) && "bg-sidebar-accent text-foreground font-medium"
            )}
          >
            <span className="flex-1 text-left">{item.name}</span>
            {isExpanded ? (
              <ChevronDown className="w-4 h-4 text-muted-foreground" />
            ) : (
              <ChevronRight className="w-4 h-4 text-muted-foreground" />
            )}
          </button>
          {isExpanded && (
            <div className="ml-3 mt-1 space-y-1 border-l border-sidebar-border pl-2">
              {nestedItems.map((nestedItem) => (
                <Link 
                  key={nestedItem.path}
                  href={nestedItem.path}
                  className={cn(
                    "flex items-center px-3 py-2 text-sm rounded-md transition-colors",
                    "text-muted-foreground hover:text-foreground hover:bg-sidebar-accent",
                    isActive(nestedItem.path) && "bg-sidebar-accent text-foreground font-medium"
                  )}
                >
                  {nestedItem.name}
                </Link>
              ))}
            </div>
          )}
        </div>
      );
    }

    return (
      <Link
        key={item.path}
        href={item.path}
        className={cn(
          "flex items-center px-3 py-2 text-sm rounded-md transition-colors",
          "text-muted-foreground hover:text-foreground hover:bg-sidebar-accent",
          isActive(item.path) && "bg-sidebar-accent text-foreground font-medium"
        )}
      >
        {item.name}
      </Link>
    );
  };

  return (
    <div className="flex flex-col h-full w-56 bg-sidebar border-r border-sidebar-border">
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-4 border-b border-sidebar-border">
        <button
          onClick={onClose}
          className="p-1 rounded-md hover:bg-sidebar-accent transition-colors"
        >
          <ChevronLeft className="w-4 h-4 text-muted-foreground" />
        </button>
        <span className="font-medium text-foreground text-sm">{title}</span>
      </div>

      {/* Submenu Items */}
      <nav className="flex-1 overflow-y-auto py-3 px-3 space-y-1">
        {items.map(renderItem)}
      </nav>
    </div>
  );
}
