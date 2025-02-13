"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoTrashOutline } from "react-icons/io5";
import {
  callGetAllDatasets,
  callRemoveDataset,
} from "@/app/services/dataset/datasetService";
import { callGetAllBuckets } from "@/app/services/bucket/bucketService";
import Table from "@/app/components/Table";
import TableRow from "@/app/components/TableRow";
import { useRouter, useSearchParams } from "next/navigation";

interface Bucket {
  name: string;
  creation_date: Date;
}

export default function DatasetPage() {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [selectedBucket, setSelectedBucket] = useState<string>("");

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

  const handleRowClick = (bucketName: string, datasetName: string) => {
    router.push(`/pages/class?bucket=${bucketName}&dataset=${datasetName}`);
  };

  if (loading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <div>{error}</div>;
  }

  return (
    <div>
      <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
        <label
          htmlFor="bucket-select"
          className="block mb-2 text-sm font-medium text-gray-900 dark:text-white"
        >
          Select Bucket
        </label>
        <select
          id="bucket-select"
          value={selectedBucket}
          onChange={(e) => setSelectedBucket(e.target.value)}
          className="bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
        >
          <option value="">Choose a bucket</option>
          {buckets.map((bucket) => (
            <option key={bucket.name} value={bucket.name}>
              {bucket.name}
            </option>
          ))}
        </select>
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
    </div>
  );
}
