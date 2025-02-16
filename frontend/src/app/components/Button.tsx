import React, { ReactNode } from "react";

interface ButtonProps {
  type?: "button" | "submit" | "reset";
  onClick?: () => void;
  className?: string;
  children: ReactNode;
}

const Button = ({
  type = "button",
  onClick,
  className = "",
  children,
}: ButtonProps) => {
  return (
    <button
      type={type}
      onClick={onClick}
      className={`py-2 px-3 text-sm font-medium text-center text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:outline-none focus:ring-blue-300 rounded-lg w-full sm:w-auto px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800 ${className}`}
    >
      {children}
    </button>
  );
};

export default Button;
