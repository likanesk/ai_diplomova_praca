import { ReactNode } from "react";

interface TableProps {
  headers: string[];
  children: ReactNode;
  caption?: string;
  description?: string;
}

export default function Table({
  headers,
  children,
  caption,
  description,
}: TableProps) {
  return (
    <div className="relative overflow-x-auto shadow-md sm:rounded-lg">
      <table className="w-[80%] mx-auto my-4 text-sm text-left rtl:text-right text-gray-500 dark:text-gray-400">
        {caption && (
          <caption className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
            {caption}
            {description && (
              <p className="mt-1 text-sm font-normal text-gray-500 dark:text-gray-400">
                {description}
              </p>
            )}
          </caption>
        )}
        <thead className="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
          <tr>
            {headers.map((header, index) => (
              <th key={index} scope="col" className="px-6 py-3">
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}
