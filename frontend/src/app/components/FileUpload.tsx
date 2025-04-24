import React from "react";

interface FileUploadProps {
  id: string;
  label: string;
  helpText: string;
  onChange: (file: File | null) => void;
  maxLength?: number;
}

function FileUpload({
  id,
  label,
  helpText,
  onChange,
  maxLength = 20,
}: FileUploadProps) {
  const [fileName, setFileName] = React.useState("");

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0] || null;
    setFileName(file?.name || "");
    onChange(file);
  };

  const truncateFileName = (name: string) => {
    if (name.length <= maxLength) return name;
    const extensionIndex = name.lastIndexOf(".");
    const extension =
      extensionIndex !== -1 ? name.substring(extensionIndex) : "";
    const baseName =
      extensionIndex !== -1 ? name.substring(0, extensionIndex) : name;

    const charsToShow = maxLength - extension.length - 3;
    if (charsToShow <= 0) return `...${extension}`;

    return `${baseName.substring(0, charsToShow)}...${extension}`;
  };

  return (
    <div>
      <label
        className="block mb-2 text-sm font-medium text-gray-900 dark:text-white"
        htmlFor={id}
      >
        {label}
      </label>
      <div className="flex items-center">
        <label
          htmlFor={id}
          className="px-4 py-2 text-sm font-medium text-gray-900 bg-gray-50 border border-gray-300 rounded-l-lg cursor-pointer dark:bg-gray-700 dark:border-gray-600 dark:text-white"
        >
          Choose file
        </label>
        <span
          className="px-4 py-2 text-sm text-gray-900 bg-gray-100 border border-l-0 border-gray-300 rounded-r-lg dark:bg-gray-600 dark:border-gray-600 dark:text-white truncate"
          title={fileName}
        >
          {fileName ? truncateFileName(fileName) : "No file chosen"}
        </span>
        <input
          className="hidden"
          id={id}
          type="file"
          onChange={handleFileChange}
        />
      </div>
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
