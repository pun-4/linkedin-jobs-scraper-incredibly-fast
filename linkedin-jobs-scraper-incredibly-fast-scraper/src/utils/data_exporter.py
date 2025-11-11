thonimport json
import logging
import os
from typing import Any, Dict, Iterable, List

import pandas as pd
from xml.etree.ElementTree import Element, SubElement, ElementTree

logger = logging.getLogger("data_exporter")

class DataExporter:
    @staticmethod
    def _ensure_parent_dir(path: str) -> None:
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)

    @staticmethod
    def export(data: Iterable[Dict[str, Any]], fmt: str, path: str) -> None:
        fmt = fmt.lower()
        records: List[Dict[str, Any]] = list(data)
        if not records:
            raise ValueError("No records to export.")

        DataExporter._ensure_parent_dir(path)

        if fmt == "json":
            DataExporter._export_json(records, path)
        elif fmt == "csv":
            DataExporter._export_csv(records, path)
        elif fmt == "excel":
            DataExporter._export_excel(records, path)
        elif fmt == "xml":
            DataExporter._export_xml(records, path)
        elif fmt == "html":
            DataExporter._export_html(records, path)
        else:
            raise ValueError(f"Unsupported export format: {fmt}")

    @staticmethod
    def _export_json(records: List[Dict[str, Any]], path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        logger.info("Written JSON to %s", path)

    @staticmethod
    def _export_csv(records: List[Dict[str, Any]], path: str) -> None:
        df = pd.DataFrame.from_records(records)
        df.to_csv(path, index=False)
        logger.info("Written CSV to %s", path)

    @staticmethod
    def _export_excel(records: List[Dict[str, Any]], path: str) -> None:
        df = pd.DataFrame.from_records(records)
        df.to_excel(path, index=False)
        logger.info("Written Excel to %s", path)

    @staticmethod
    def _export_xml(records: List[Dict[str, Any]], path: str) -> None:
        root = Element("jobs")
        for rec in records:
            job_el = SubElement(root, "job")
            for key, value in rec.items():
                field_el = SubElement(job_el, key)
                if isinstance(value, list):
                    field_el.text = ", ".join(str(v) for v in value)
                else:
                    field_el.text = "" if value is None else str(value)
        tree = ElementTree(root)
        tree.write(path, encoding="utf-8", xml_declaration=True)
        logger.info("Written XML to %s", path)

    @staticmethod
    def _export_html(records: List[Dict[str, Any]], path: str) -> None:
        df = pd.DataFrame.from_records(records)
        html = df.to_html(index=False, border=0, classes="linkedin-jobs-table")
        with open(path, "w", encoding="utf-8") as f:
            f.write("<!DOCTYPE html>\n<html><head><meta charset='utf-8'><title>LinkedIn Jobs</title></head><body>")
            f.write(html)
            f.write("</body></html>")
        logger.info("Written HTML to %s", path)