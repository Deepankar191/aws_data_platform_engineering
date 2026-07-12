"""Pytest bootstrap. Puts glue/ on sys.path so tests can import the Spark-free
``common.*`` modules (constants, text_match) directly, and exposes repo-root paths."""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GLUE_DIR = os.path.join(REPO_ROOT, "glue")

if GLUE_DIR not in sys.path:
    sys.path.insert(0, GLUE_DIR)


def repo_path(*parts):
    """Absolute path to a file/dir under the repo root."""
    return os.path.join(REPO_ROOT, *parts)
