import { ReactNode, MouseEvent } from "react";

interface IconButtonProps {
  icon: ReactNode;
  onClick: (e?: MouseEvent) => void;
  className?: string;
  position?: "absolute" | "relative" | "static";
  top?: string; // Position top (if position: absolute)
  right?: string; // Position right (if position: absolute)
}

export default function IconButton({
  icon,
  onClick,
  className = "",
  position = "static",
  top = "auto",
  right = "auto",
}: IconButtonProps) {
  return (
    <button
      onClick={(e) => onClick(e)}
      className={`${position} ${top} ${right} text-gray-400 hover:bg-gray-200 hover:text-blue-500 rounded-lg text-sm w-8 h-8 flex items-center justify-center dark:hover:bg-gray-600 dark:hover:text-white ${className}`}
    >
      {icon}
    </button>
  );
}
