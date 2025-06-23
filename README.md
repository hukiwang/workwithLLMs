# workwithLLMs

This repository contains utilities for transforming SAP HANA procedure SQL
scripts so they can run on Huawei DWS. The original implementation was a Java
program (`ProcedureBatchParserProc1.java`). A Python equivalent
(`procedure_batch_parser_proc1.py`) is now provided for easier usage.

Usage:

```bash
python procedure_batch_parser_proc1.py /path/to/Input
```

The script will scan the given input directory for `.sql` files and generate
converted SQL under sibling `output`, `outputAll`, and `outputDropProcAll`
folders.
