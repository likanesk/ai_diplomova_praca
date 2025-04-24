import os
import json
from typing import Dict, List

class CommonValidator:
    """
    This validator contains common logic for validating the structure and content of ZIP archives specially for RegressionValidator and DetectionValidator.
    """
    def __init__(self, valid_extensions: set):
        self.valid_extensions = valid_extensions

    def is_valid_image(self, file_name: str) -> bool:
        _, ext = os.path.splitext(file_name)
        return ext in self.valid_extensions

    def validate_zip_structure(self, temp_dir: str) -> Dict[str, List]:
        """
        Validates the structure of the ZIP archive. 
        ZIP archive contains only one top-level folder (DB folder) and no other folders.
        """
        errors = {"invalid_structure": []}
        db_folder = None

        # Validate top-level folder
        for current_folder, subfolders, _ in os.walk(temp_dir):
            rel_path = os.path.relpath(current_folder, temp_dir)
            if rel_path == ".":
                if len(subfolders) != 1:
                    errors["invalid_structure"].append("There should be exactly one top-level <DB> folder.")
                    return errors
                db_folder = subfolders[0]

        # Validate DB folder content
        db_path = os.path.join(temp_dir, db_folder)
        for item in os.listdir(db_path):
            if os.path.isdir(os.path.join(db_path, item)):
                errors["invalid_structure"].append(f"The '{db_folder}' folder should not contain subfolders.")
        
        return errors

    async def validate_zip_contents(self, temp_dir: str, is_regression: bool) -> Dict[str, List]:
        """
        Validates the contents of the ZIP archive. 
        It checks that a JSON file exists. 
        The number of image records in the JSON file must be equal to the total number of images in the folder, and vice versa.
        """
        errors = {"missing_images": [], "missing_annotations": [], "invalid_data": [], "invalid_structure": []}
        
        # Validate structure
        structure_errors = self.validate_zip_structure(temp_dir)
        if structure_errors["invalid_structure"]:
            return structure_errors

        # Load JSON data
        db_folder = os.listdir(temp_dir)[0]
        db_path = os.path.join(temp_dir, db_folder)
        json_files = [f for f in os.listdir(db_path) if f.endswith(".json")]
        
        if not json_files:
            errors["invalid_structure"].append("JSON file not found in the DB folder.")
            return errors
        
        json_path = os.path.join(db_path, json_files[0])
        
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                errors["invalid_structure"].append("Invalid JSON format.")
                return errors

        # Validate data consistency
        image_files = [f for f in os.listdir(db_path) if self.is_valid_image(f)]
        image_names_in_json = [item["file"] for item in data if "file" in item]

        # Check missing images
        errors["missing_images"] = [img for img in image_names_in_json if img not in image_files]
        errors["missing_annotations"] = [img for img in image_files if img not in image_names_in_json]

        return errors