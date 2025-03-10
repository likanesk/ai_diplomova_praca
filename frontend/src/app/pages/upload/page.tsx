"use client";

import { useState, useCallback, useEffect } from "react";
import { IoClose, IoInformationOutline } from "react-icons/io5";
import {
  callUploadZipForClassification,
  callUploadZipForRegression,
  callUploadZipForDetection,
} from "@/app/services/dataset/datasetService";
import { callGetAllBuckets } from "@/app/services/bucket/bucketService";
import Dropdown from "@/app/components/Dropdown";
import InputField from "@/app/components/InputField";
import Button from "@/app/components/Button";
import FileUpload from "@/app/components/FileUpload";
import SuccessMessage from "@/app/components/SuccessMessage";
import ErrorMessage from "@/app/components/ErrorMessage";
import { useAuth } from "@/app/hooks/useAuth";
import IconButton from "@/app/components/IconButton";
import Image from "next/image";

interface Bucket {
  name: string;
  creation_date: Date;
}

const validationTypes = [
  { value: "classification", label: "Classification" },
  { value: "regression", label: "Regression" },
  { value: "detection", label: "Detection" },
];

export default function UploadPage() {
  useAuth();

  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [validationType, setValidationType] =
    useState<string>("classification");
  const [inputBucketName, setInputBucketName] = useState("");
  const [inputExpectedNumClasses, setInputExpectedNumClasses] = useState("");
  const [inputExpectedNumFilesPerClass, setInputExpectedNumFilesPerClass] =
    useState("");
  const [inputFile, setInputFile] = useState<File | null>(null);
  const [showSuccessModal, setShowSuccessModal] = useState(false);
  const [showExample, setShowExample] = useState(false);

  const fetchBuckets = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await callGetAllBuckets();
      setBuckets(data?.buckets || []);
    } catch {
      setError("Failed to fetch buckets");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  const handleUploadZip = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!inputFile || !inputBucketName) {
      setError("Please select a file and a bucket.");
      return;
    }

    try {
      if (validationType === "classification") {
        await callUploadZipForClassification(
          inputBucketName,
          parseInt(inputExpectedNumClasses),
          parseInt(inputExpectedNumFilesPerClass),
          inputFile
        );
      } else if (validationType === "regression") {
        await callUploadZipForRegression(inputBucketName, inputFile);
      } else if (validationType === "detection") {
        await callUploadZipForDetection(inputBucketName, inputFile);
      }

      setInputBucketName("");
      setInputExpectedNumClasses("");
      setInputExpectedNumFilesPerClass("");
      setInputFile(null);
      setShowSuccessModal(true);
      setError("");
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : `Failed to upload dataset in bucket: ${inputBucketName}`
      );
    }
  };

  const isUploadDisabled =
    !validationType ||
    !inputBucketName ||
    !inputFile ||
    (validationType === "classification" &&
      (!inputExpectedNumClasses || !inputExpectedNumFilesPerClass));

  if (loading) {
    return <div>Loading...</div>;
  }

  return (
    <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
      <h1 className="pb-5 text-center">Upload dataset</h1>

      {error && <ErrorMessage message={error} onClose={() => setError("")} />}

      <div className="flex justify-center items-center gap-4">
        <div>
          <form
            className="max-w-sm mx-auto my-4 flex flex-col "
            onSubmit={handleUploadZip}
          >
            <Dropdown
              id="validation-type"
              label="Select Validation Type"
              value={validationType}
              onChange={setValidationType}
              options={validationTypes}
              placeholder="Choose validation type"
            />
            <Dropdown
              id="bucket-dropdown"
              label="Select Bucket to upload db"
              value={inputBucketName}
              onChange={setInputBucketName}
              options={buckets.map((bucket) => ({
                value: bucket.name,
                label: bucket.name,
              }))}
              placeholder="Choose a bucket"
            />
            {validationType === "classification" && (
              <>
                <InputField
                  label="Expected number of classes"
                  type="text"
                  id="classcount"
                  name="classcount"
                  required
                  value={inputExpectedNumClasses}
                  onChange={(value) => setInputExpectedNumClasses(value)}
                  pattern="^[0-9]*$"
                />
                <InputField
                  label="Expected number of samples in class"
                  type="text"
                  id="samplecount"
                  name="samplecount"
                  required
                  value={inputExpectedNumFilesPerClass}
                  onChange={(value) => setInputExpectedNumFilesPerClass(value)}
                  pattern="^[0-9]*$"
                />
              </>
            )}
            <div className="flex">
              <FileUpload
                id="file_input"
                label="Upload file"
                helpText=".ZIP file is accepted"
                onChange={(file) => setInputFile(file)}
              />
              <IconButton
                icon={<IoInformationOutline className="w-5 h-5" />}
                onClick={(e) => {
                  e?.preventDefault();
                  setShowExample(!showExample);
                }}
                className="ml-2"
              />
            </div>

            <Button
              className="mx-auto mt-2"
              type="submit"
              disabled={isUploadDisabled}
            >
              Upload
            </Button>
          </form>
        </div>

        <div className="flex flex-col items-center">
          {showExample && (
            <div className="relative">
              {validationType === "classification" ? (
                <Image
                  src="/zip-classification-structure.png"
                  width={220}
                  height={220}
                  alt="Example of ZIP file structure for classification"
                  className="md:ml-10 mt-8 md:mt-0"
                />
              ) : (
                <div className="flex items-start space-x-4 mt-8">
                  <Image
                    src="/zip-regression-and-detection-structure.png"
                    width={250}
                    height={250}
                    alt="Example of ZIP file structure for regression and detection"
                    className="flex-none"
                  />

                  {validationType === "regression" && (
                    <Image
                      src="/json-regression.png"
                      width={450}
                      height={250}
                      alt="Example of JSON file structure for regression"
                      className="flex-none"
                    />
                  )}

                  {validationType === "detection" && (
                    <Image
                      src="/json-detection.png"
                      width={250}
                      height={200}
                      alt="Example of JSON file structure for detection"
                      className="flex-none"
                    />
                  )}
                </div>
              )}

              <IconButton
                icon={<IoClose />}
                onClick={() => setShowExample(false)}
                position="absolute"
                top="top-0"
                right="right-0"
              />
            </div>
          )}
        </div>
      </div>

      {showSuccessModal && (
        <SuccessMessage
          onClose={() => setShowSuccessModal(false)}
          message={`Dataset was successfully added into bucket!`}
        />
      )}
    </div>
  );
}
