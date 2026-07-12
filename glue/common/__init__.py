"""Shared utilities for the credit-decision Glue transformation layer.

Packaged and shipped to Glue jobs via ``--extra-py-files`` (as ``common.zip``)
so that every job can ``from common import ...``. The same package runs
unchanged on EMR — see ``glue/README.md``.
"""
