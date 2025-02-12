import { ReactNode } from "react";

interface TableRowProps {
  children: ReactNode;
  onClick?: () => void;
}

export default function TableRow({ children, onClick }: TableRowProps) {
  return (
    <tr
      onClick={onClick}
      className="bg-white border-b dark:bg-gray-800 dark:border-gray-700 border-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600 cursor-pointer"
    >
      {children}
    </tr>
  );
}
