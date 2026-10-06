#!/usr/bin/env python3
"""Check source completeness in a static Pages artifact or a local candidate.

Without --artifact, copies only active assets and declared sources to a temporary
candidate, using git's tracked + normally addable file inventory. This catches
ignored required sources without staging, committing, or deploying anything.
With --artifact DIR, validates the actual local publication directory instead.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
ACTIVE_FILES = ('index.html', 'favicon.svg', 'assets/evidence.js',
                'assets/evidence.css', 'data/evidence.json', 'EVIDENCE.md')


def required_files(docs):
    data = json.loads((docs / 'data/evidence.json').read_text())
    paths = set(ACTIVE_FILES)
    for source in data['sources'].values():
        relative = Path(source['path'])
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError(f'Unsafe source path: {relative}')
        paths.add(relative.as_posix())
    return paths


def check_git_eligibility(root=ROOT):
    """Tracked files plus normally addable untracked files; no force-add loophole."""
    listed = subprocess.check_output(
        ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z', '--', 'docs'], cwd=root)
    eligible = set(listed.decode().split('\0'))
    required = required_files(root / 'docs')
    missing = sorted('docs/' + p for p in required if 'docs/' + p not in eligible)
    if missing:
        raise ValueError('Required files excluded from normal source packaging: ' + ', '.join(missing))
    return required


def validate_artifact(docs):
    for name in required_files(docs):
        if not (docs / name).is_file():
            raise ValueError(f'Missing publication file: {name}')
    data = json.loads((docs / 'data/evidence.json').read_text())
    for name, source in data['sources'].items():
        payload = (docs / source['path']).read_bytes()
        if len(payload) != source['bytes'] or hashlib.sha256(payload).hexdigest() != source['sha256']:
            raise ValueError(f'Publication source hash mismatch: {name}')
    return len(data['sources'])


def stage_candidate(destination, root=ROOT):
    for name in sorted(check_git_eligibility(root)):
        source, target = root / 'docs' / name, destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    validate_artifact(destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, help='Existing static publication directory; read-only validation')
    args = parser.parse_args()
    if args.artifact:
        count = validate_artifact(args.artifact)
        mode = 'provided publication artifact'
    else:
        with tempfile.TemporaryDirectory(prefix='uuid-pages-candidate-') as directory:
            candidate = Path(directory)
            stage_candidate(candidate)
            count = validate_artifact(candidate)
        mode = 'local Git-eligible candidate (not a deployment)'
    print(json.dumps({'status': 'passed', 'mode': mode, 'sources': count,
                      'checks': 'active assets, source completeness, exact source hashes'}))


if __name__ == '__main__':
    main()
