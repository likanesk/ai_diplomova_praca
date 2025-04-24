"use client";

import Link from "next/link";
import { ReactNode, MouseEvent } from "react";

interface CustomLinkProps {
  href: string;
  children: ReactNode;
  className?: string;
  icon?: ReactNode;
  iconPosition?: "left" | "right";
  useFlex?: boolean;
  onClick?: (event: MouseEvent<HTMLAnchorElement>) => void;
}

export default function CustomLink({
  href,
  children,
  className = "",
  icon,
  iconPosition = "left",
  useFlex = false,
  onClick,
}: CustomLinkProps) {
  const handleClick = (event: MouseEvent<HTMLAnchorElement>) => {
    if (onClick) {
      event.preventDefault();
      onClick(event);
    }
  };

  return (
    <Link
      href={href}
      className={`text-sm text-blue-600 dark:text-blue-500 hover:underline ${className} ${
        useFlex ? "flex items-center" : ""
      }`}
      onClick={handleClick}
    >
      {icon && iconPosition === "left" && <span className="mr-1">{icon}</span>}
      {children}
      {icon && iconPosition === "right" && <span className="ml-1">{icon}</span>}
    </Link>
  );
}
