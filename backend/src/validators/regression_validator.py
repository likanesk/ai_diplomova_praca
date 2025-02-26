import json
import os
from .common_validator import CommonValidator
from typing import Dict, List

class RegressionValidator(CommonValidator):
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
        It uses methods from CommonValidator for structure and content validation and adds specific checks for regression values.
        """
        errors = await self.validate_zip_contents(temp_dir, is_regression=True)
        
        if any(errors.values()):
            return errors
        
        # Load JSON data
        db_folder = os.listdir(temp_dir)[0]
        db_path = os.path.join(temp_dir, db_folder)
        json_files = [f for f in os.listdir(db_path) if f.endswith(".json")]
        
        json_path = os.path.join(db_path, json_files[0])
        
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                return errors
        
        data_errors = self.validate_regression_data(data)
        errors.update(data_errors)
        return errors