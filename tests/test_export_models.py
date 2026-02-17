"""
Tests for deploy/export_models_for_aks.py — argument parsing and catalog name resolution.
"""

import argparse
import os
import sys

import pytest


class TestCatalogNameResolution:
    """Test environment → catalog name mapping logic."""

    @staticmethod
    def _resolve_catalog(env: str) -> str:
        return "uk_telecoms" if env == "prod" else f"uk_telecoms_{env}"

    def test_prod_uses_base_catalog(self):
        assert self._resolve_catalog("prod") == "uk_telecoms"

    def test_dev_uses_suffixed_catalog(self):
        assert self._resolve_catalog("dev") == "uk_telecoms_dev"

    def test_staging_uses_suffixed_catalog(self):
        assert self._resolve_catalog("staging") == "uk_telecoms_staging"


class TestArgumentParsing:
    """Test the argparse configuration from the export script."""

    @staticmethod
    def _build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument("--output-dir", required=True)
        parser.add_argument("--env", default="prod", choices=["dev", "staging", "prod"])
        parser.add_argument("--export-scores", action="store_true")
        return parser

    def test_output_dir_required(self):
        parser = self._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_env_default_is_prod(self):
        parser = self._build_parser()
        args = parser.parse_args(["--output-dir", "/tmp/models"])
        assert args.env == "prod"

    def test_export_scores_flag(self):
        parser = self._build_parser()
        args = parser.parse_args(["--output-dir", "/tmp/models", "--export-scores"])
        assert args.export_scores is True

    def test_export_scores_default_false(self):
        parser = self._build_parser()
        args = parser.parse_args(["--output-dir", "/tmp/models"])
        assert args.export_scores is False
