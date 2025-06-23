import tempfile
from pathlib import Path
import filecmp

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from procedure_batch_parser_proc1 import main


def test_sample_conversion():
    base = Path('testSample/input').resolve()
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / 'input'
        input_dir.mkdir()
        (input_dir / '001_in.sql').write_text((base / '001_in.sql').read_text(encoding='utf-8'), encoding='utf-8')
        main(str(input_dir))
        output_file = input_dir.parent / 'output' / '001_in.sql'
        expected = Path('testSample/ouput/001_out.sql').resolve()
        assert filecmp.cmp(output_file, expected, shallow=False)
