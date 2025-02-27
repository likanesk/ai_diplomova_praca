"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoTrashOutline } from "react-icons/io5";
import {
  callGetAllClasses,
  callRemoveClass,
} from "@/app/services/class/classService";
import { callGetAllBuckets } from "@/app/services/bucket/bucketService";
import { callGetAllDatasets } from "@/app/services/dataset/datasetService";
import Table from "@/app/components/Table";
import TableRow from "@/app/components/TableRow";
import { useRouter, useSearchParams } from "next/navigation";
import Dropdown from "@/app/components/Dropdown";
import { useAuth } from "@/app/hooks/useAuth";
import ErrorMessage from "@/app/components/ErrorMessage";

interface Bucket {
  name: string;
  creation_date: Date;
}

export default function ClassPage() {
  useAuth();

  const [classes, setClasses] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [datasets, setDatasets] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedClass, setSelectedClass] = useState<string | null>(null);
  const [selectedBucket, setSelectedBucket] = useState<string>("");
  const [selectedDataset, setSelectedDataset] = useState<string>("");

  const [noClassesError, setNoClassesError] = useState(false);

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
    if (!selectedBucket) {
      setDatasets([]);
      return;
    }

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

  const fetchClasses = useCallback(async () => {
    if (!selectedBucket || !selectedDataset) {
      return;
    }

    setLoading(true);
    setError("");
    setNoClassesError(false);

    try {
      const data = await callGetAllClasses(selectedBucket, selectedDataset);
      const fetchedClasses = data?.classes || [];

      setClasses(fetchedClasses);
      if (fetchedClasses.length === 0) {
        setNoClassesError(true);
      }
    } catch {
      setError("Failed to fetch classes");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket, selectedDataset]);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  useEffect(() => {
    setDatasets([]);
    setClasses([]);
    fetchDatasets();
  }, [selectedBucket, fetchDatasets]);

  useEffect(() => {
    fetchClasses();
  }, [fetchClasses, selectedBucket, selectedDataset]);

  useEffect(() => {
    const bucket = searchParams.get("bucket");
    const dataset = searchParams.get("dataset");
    if (bucket) {
      setSelectedBucket(bucket);
    }
    if (dataset) {
      setSelectedDataset(dataset);
    }
  }, [searchParams]);

  const handleDelete = async () => {
    if (selectedClass && selectedBucket && selectedDataset) {
      try {
        await callRemoveClass(selectedBucket, selectedDataset, selectedClass);

        setClasses((prevClasses) =>
          prevClasses.filter((cls) => cls !== selectedClass)
        );

        setIsModalOpen(false);
      } catch {
        setError(`Failed to delete class: ${selectedClass}`);
      }
    }
  };

  const handleRowClick = (
    bucketName: string,
    datasetName: string,
    className: string
  ) => {
    router.push(
      `/pages/sample?bucket=${bucketName}&dataset=${datasetName}&class=${className}`
    );
  };

  if (loading) {
    return <div>Loading...</div>;
  }

  return (
    <div>
      <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
        <Dropdown
          id="bucket-select"
          label="Select Bucket"
          value={selectedBucket}
          onChange={(value) => {
            setSelectedBucket(value);
            setSelectedDataset("");
          }}
          options={buckets.map((bucket) => ({
            value: bucket.name,
            label: bucket.name,
          }))}
          placeholder="Choose a bucket"
        />

        <Dropdown
          id="dataset-select"
          label="Select Dataset"
          value={selectedDataset}
          onChange={(value) => setSelectedDataset(value)}
          options={datasets.map((dataset) => ({
            value: dataset,
            label: dataset,
          }))}
          placeholder="Choose a dataset"
        />
      </div>

      {noClassesError && (
        <ErrorMessage
          message="Selected dataset does not contain any class!"
          onClose={() => setNoClassesError(false)}
        />
      )}

      <Table
        headers={["Class Name", "Remove"]}
        caption="Classes"
        description="A class is a collection of data within a dataset, typically representing a specific type or category of data."
      >
        {classes.map((cls, index) => (
          <TableRow
            key={index}
            onClick={() => handleRowClick(selectedBucket, selectedDataset, cls)}
          >
            <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
              {cls}
            </td>
            <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setSelectedClass(cls);
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
        name={selectedClass || ""}
        error={error}
        onErrorClose={() => setError("")}
      />
    </div>
  );
}
