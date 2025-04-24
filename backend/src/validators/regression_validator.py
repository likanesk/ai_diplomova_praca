import json
import os
from .common_validator import CommonValidator
from typing import Dict, List

class RegressionValidator(CommonValidator):
    """
    This validator is used for validating data for regression. 
    Regression values in the JSON file must be valid and ZIP must contain exactly one JSON file.
    """
    def validate_regression_data(self, data: List[dict]) -> Dict[str, List]:
        """
        Validates regression values in the JSON file. 
        Regression values must be of type int or float.
        """
        errors = {"invalid_regression": []}
        for item in data:
            if "regression" not in item or not isinstance(item["regression"], (int, float)):
                errors["invalid_regression"].append(item.get("file", "Unknown file"))
        return errors

    async def validate(self, temp_dir: str) -> Dict[str, List]:
        """
        Validates the entire ZIP archive for regression.
        It uses methods from CommonValidator for structure and content validation,
        checks that ZIP contains exactly one JSON file,
        and adds specific checks for regression values.
        """
        errors = await self.validate_zip_contents(temp_dir, is_regression=True)
        
        if any(errors.values()):
            return errors
        
        # Check for exactly one JSON file
        db_folder = os.listdir(temp_dir)[0]
        db_path = os.path.join(temp_dir, db_folder)
        json_files = [f for f in os.listdir(db_path) if f.endswith(".json")]
        
        if len(json_files) != 1:
            errors["invalid_structure"].append("ZIP must contain exactly one JSON file")
            return errors
        
        # Load JSON data
        json_path = os.path.join(db_path, json_files[0])
        
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                errors["invalid_json"].append("Invalid JSON format")
                return errors
        
        data_errors = self.validate_regression_data(data)
        errors.update(data_errors)
        return errors