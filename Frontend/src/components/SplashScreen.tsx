import { useEffect, useState } from "react";
import { DiceLogo } from "./icons/DiceLogo";

interface SplashScreenProps {
  onComplete: () => void;
  minDuration?: number;
}

export function SplashScreen({ onComplete, minDuration = 2000 }: SplashScreenProps) {
  const [isExiting, setIsExiting] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => {
      setIsExiting(true);
      setTimeout(onComplete, 500);
    }, minDuration);

    return () => clearTimeout(timer);
  }, [onComplete, minDuration]);

  return (
    <div
      className={`fixed inset-0 z-50 flex flex-col items-center justify-center bg-background transition-opacity duration-500 ${
        isExiting ? "opacity-0" : "opacity-100"
      }`}
    >
      {/* Background gradient */}
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-background to-primary/10" />
      
      {/* Animated rings */}
      <div className="absolute">
        <div className="w-40 h-40 rounded-full border border-primary/20 animate-[ping_2s_ease-in-out_infinite]" />
      </div>
      <div className="absolute">
        <div className="w-56 h-56 rounded-full border border-primary/10 animate-[ping_2s_ease-in-out_infinite_0.5s]" />
      </div>
      
      {/* Logo container */}
      <div className="relative z-10 flex flex-col items-center gap-6">
        {/* Logo with glow effect */}
        <div className="relative">
          <div className="absolute inset-0 blur-2xl bg-primary/30 rounded-full scale-150" />
          <div className="relative w-20 h-20 rounded-2xl bg-primary flex items-center justify-center shadow-2xl shadow-primary/30 animate-[scale-in_0.5s_ease-out]">
            <DiceLogo size={48} className="text-primary-foreground" />
          </div>
        </div>
        
        {/* Brand name with stagger animation */}
        <div className="flex flex-col items-center gap-2 animate-[fade-in_0.5s_ease-out_0.3s_both]">
          <h1 className="text-4xl font-bold tracking-tight text-foreground">
            DICE
          </h1>
          <p className="text-sm text-muted-foreground tracking-widest uppercase">
            Data, Intelligence & Cognitive Engine
          </p>
        </div>
        
        {/* Loading indicator */}
        <div className="flex gap-1.5 mt-4 animate-[fade-in_0.5s_ease-out_0.6s_both]">
          <div className="w-2 h-2 rounded-full bg-primary animate-[bounce_1s_ease-in-out_infinite]" />
          <div className="w-2 h-2 rounded-full bg-primary animate-[bounce_1s_ease-in-out_infinite_0.1s]" />
          <div className="w-2 h-2 rounded-full bg-primary animate-[bounce_1s_ease-in-out_infinite_0.2s]" />
        </div>
      </div>
    </div>
  );
}
