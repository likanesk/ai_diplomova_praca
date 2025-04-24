import React, { useEffect } from "react";
import IconButton from "./IconButton";
import { IoClose } from "react-icons/io5";

interface ErrorMessageProps {
  message: string;
  onClose?: () => void;
}

function ErrorMessage({ message, onClose }: ErrorMessageProps) {
  useEffect(() => {
    if (onClose) {
      const timer = setTimeout(() => {
        onClose();
      }, 3500);

      return () => clearTimeout(timer);
    }
  }, [onClose]);

  return (
    <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative max-w-sm mx-auto my-4">
      <span className="block sm:inline">{message}</span>
      {onClose && (
        <IconButton
          icon={<IoClose className="w-4 h-4" />}
          onClick={onClose}
          position="absolute"
          top="top-2"
          right="right-2"
        />
      )}
    </div>
  );
}

export default ErrorMessage;
