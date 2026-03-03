import { cn } from "@/lib/utils";

interface DiceLogoProps {
  className?: string;
  size?: number;
}

export function DiceLogo({ className, size = 24 }: DiceLogoProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={cn("", className)}
    >
      <defs>
        <linearGradient id="cubeGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="currentColor" stopOpacity="1" />
          <stop offset="100%" stopColor="currentColor" stopOpacity="0.6" />
        </linearGradient>
      </defs>
      
      {/* Stylized "D" with data cube aesthetic */}
      <path
        d="M8 6C8 4.89543 8.89543 4 10 4H18C22.4183 4 26 7.58172 26 12V20C26 24.4183 22.4183 28 18 28H10C8.89543 28 8 27.1046 8 26V6Z"
        fill="url(#cubeGradient)"
        opacity="0.15"
      />
      
      {/* Inner geometric pattern - represents data/intelligence */}
      <path
        d="M10 8H18C20.2091 8 22 9.79086 22 12V20C22 22.2091 20.2091 24 18 24H10V8Z"
        stroke="currentColor"
        strokeWidth="2"
        fill="none"
      />
      
      {/* Data nodes */}
      <circle cx="14" cy="12" r="2" fill="currentColor" />
      <circle cx="18" cy="16" r="2" fill="currentColor" />
      <circle cx="14" cy="20" r="2" fill="currentColor" />
      
      {/* Connection lines */}
      <line x1="14" y1="12" x2="18" y2="16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      <line x1="18" y1="16" x2="14" y2="20" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}
