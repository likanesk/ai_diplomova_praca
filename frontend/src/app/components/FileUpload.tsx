import React from "react";

interface FileUploadProps {
  id: string;
  label: string;
  helpText: string;
  onChange: (file: File | null) => void;
}

function FileUpload({ id, label, helpText, onChange }: FileUploadProps) {
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0] || null;
    onChange(file);
  };

  return (
    <div>
      <label
        className="block mb-2 text-sm font-medium text-gray-900 dark:text-white"
        htmlFor={id}
      >
        {label}
      </label>
      <input
        className="block w-full text-sm text-gray-900 border border-gray-300 rounded-lg cursor-pointer bg-gray-50 dark:text-gray-400 focus:outline-none dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400"
        aria-describedby={`${id}_help`}
        id={id}
        type="file"
        onChange={handleFileChange}
      />
      <p
        className="mt-1 text-sm text-gray-500 dark:text-gray-300"
        id={`${id}_help`}
      >
        {helpText}
      </p>
    </div>
  );
}

export default FileUpload;
