import React from "react";
import CloseButton from "./CloseButton";

interface ErrorMessageProps {
  message: string;
  onClose?: () => void;
}

function ErrorMessage({ message, onClose }: ErrorMessageProps) {
  return (
    <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative max-w-sm mx-auto my-4">
      <span className="block sm:inline">{message}</span>
      {onClose && (
        <CloseButton className="fill-current text-red-500" onClose={onClose} />
      )}
    </div>
  );
}

export default ErrorMessage;
