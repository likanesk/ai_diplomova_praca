"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import Image from "next/image";
import { useEffect, useState, useCallback } from "react";
import { IoClose, IoInformationOutline, IoTrashOutline } from "react-icons/io5";
import {
  callGetAllDatasets,
  callRemoveDataset,
  callUploadZipForClassification,
  callUploadZipForDetection,
  callUploadZipForRegression,
} from "@/app/services/dataset/datasetService";
import { callGetAllBuckets } from "@/app/services/bucket/bucketService";
import Table from "@/app/components/Table";
import TableRow from "@/app/components/TableRow";
import { useRouter, useSearchParams } from "next/navigation";
import InputField from "@/app/components/InputField";
import Button from "@/app/components/Button";
import Dropdown from "@/app/components/Dropdown";
import FileUpload from "@/app/components/FileUpload";
import SuccessMessage from "@/app/components/SuccessMessage";
import ErrorMessage from "@/app/components/ErrorMessage";
import { useAuth } from "@/app/hooks/useAuth";
import IconButton from "@/app/components/IconButton";
import { callGetAllClasses } from "@/app/services/class/classService";

interface Bucket {
  name: string;
  creation_date: Date;
}

const validationTypes = [
  { value: "classification", label: "Classification" },
  { value: "regression", label: "Regression" },
  { value: "detection", label: "Detection" },
];

export default function DatasetPage() {
  useAuth();

  const [datasets, setDatasets] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [selectedBucket, setSelectedBucket] = useState<string>("");
  const [validationType, setValidationType] =
    useState<string>("classification");
  const [inputBucketName, setInputBucketName] = useState("");
  const [inputExpectedNumClasses, setInputExpectedNumClasses] = useState("");
  const [inputExpectedNumFilesPerClass, setInputExpectedNumFilesPerClass] =
    useState("");
  const [inputFile, setInputFile] = useState<File | null>(null);
  const [showSuccessModal, setShowSuccessModal] = useState(false);
  const [showExample, setShowExample] = useState(false);

  const searchParams = useSearchParams();
  const router = useRouter();

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

  const fetchDatasets = useCallback(async () => {
    if (!selectedBucket) return;

    setLoading(true);
    setError("");
    try {
      const data = await callGetAllDatasets(selectedBucket);
      setDatasets(data?.databases || []);
    } catch {
      setError("Failed to fetch datasets");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket]);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  useEffect(() => {
    fetchDatasets();
  }, [fetchDatasets]);

  useEffect(() => {
    const bucket = searchParams.get("bucket");
    if (bucket) {
      setSelectedBucket(bucket);
    }
  }, [searchParams]);

  const handleDelete = async () => {
    if (selectedDataset && selectedBucket) {
      try {
        await callRemoveDataset(selectedBucket, selectedDataset);

        setDatasets((prevDatasets) =>
          prevDatasets.filter((dataset) => dataset !== selectedDataset)
        );

        setIsModalOpen(false);
      } catch {
        setError(`Failed to delete dataset: ${selectedDataset}`);
      }
    }
  };

  const handleRowClick = async (bucketName: string, datasetName: string) => {
    try {
      const classes = await callGetAllClasses(bucketName, datasetName);
      if (classes?.classes && classes.classes.length > 0) {
        // Classification -> navigate to ClassPage
        router.push(`/pages/class?bucket=${bucketName}&dataset=${datasetName}`);
      } else {
        // Regression or Detection -> navigate to SamplePage
        router.push(
          `/pages/sample?bucket=${bucketName}&dataset=${datasetName}`
        );
      }
    } catch {
      setError("Failed to determine dataset type");
    }
  };

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

      fetchDatasets();
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
    <div>
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
                <Image
                  src="/zip-regression-and-detection-structure.png"
                  width={250}
                  height={250}
                  alt="Example of ZIP file structure for regression and detection"
                  className="md:ml-10 mt-8 md:mt-0"
                />
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

      <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
        <Dropdown
          id="bucket-select"
          label="Select Bucket"
          value={selectedBucket}
          onChange={setSelectedBucket}
          options={buckets.map((bucket) => ({
            value: bucket.name,
            label: bucket.name,
          }))}
          placeholder="Choose a bucket"
        />
      </div>

      <Table
        headers={["Dataset Name", "Remove"]}
        caption="Datasets"
        description="A dataset is a collection of data, typically in a structured format, that can be used for analysis or processing."
      >
        {datasets.map((dataset, index) => (
          <TableRow
            key={index}
            onClick={() => handleRowClick(selectedBucket, dataset)}
          >
            <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
              {dataset}
            </td>
            <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setSelectedDataset(dataset);
                  setIsModalOpen(true);
                }}
                className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
              >
                <IoTrashOutline className="w-5 h-5 text-red-500 dark:text-white mr-1" />
              </button>
            </td>
          </TableRow>
        ))}
      </Table>

      <RemoveDialog
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onConfirm={handleDelete}
        name={selectedDataset || ""}
      />

      {showSuccessModal && (
        <SuccessMessage
          onClose={() => setShowSuccessModal(false)}
          message={`Dataset was successfully added into bucket!`}
        />
      )}
    </div>
  );
}
