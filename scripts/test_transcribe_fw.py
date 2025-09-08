#!/usr/bin/env python3
import argparse, sys
from pathlib import Path

ALIASES = {
    'tiny': 'Systran/faster-whisper-tiny',
    'base': 'Systran/faster-whisper-base',
    'small': 'Systran/faster-whisper-small',
    'medium': 'Systran/faster-whisper-medium',
    'large-v2': 'Systran/faster-whisper-large-v2',
    'large-v3': 'Systran/faster-whisper-large-v3',
}

def ensure_model(model_ref: str, cache_dir: Path) -> str:
    p = Path(model_ref).expanduser()
    if p.exists() and p.is_dir():
        return str(p)
    repo = ALIASES.get(model_ref.lower()) if model_ref else None
    if not repo:
        if '/' in (model_ref or ''):
            repo = model_ref
        else:
            return ''
    try:
        from huggingface_hub import snapshot_download
    except Exception:
        print('huggingface_hub is not installed. Run: pip install huggingface_hub', file=sys.stderr)
        return ''
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe = repo.replace('/','__')
    target = cache_dir / safe
    if not target.exists() or not any(target.iterdir()):
        print(f'Downloading {repo} to {target} ...')
        snapshot_download(repo_id=repo, local_dir=str(target), local_dir_use_symlinks=False)
    return str(target)

def main():
    ap = argparse.ArgumentParser(description='faster-whisper transcription test')
    ap.add_argument('--model', required=True, help='Path to CTranslate2 model dir or alias (small, large-v3, or org/repo)')
    ap.add_argument('--lang', default='ru', help='Language code (or empty for auto)')
    ap.add_argument('--no-vad', action='store_true', help='Disable VAD filter')
    ap.add_argument('--cache', default='./models/faster-whisper', help='Cache dir for auto-downloaded models')
    ap.add_argument('audio', help='Path to input audio file')
    args = ap.parse_args()

    try:
        from faster_whisper import WhisperModel
    except Exception as e:
        print('faster-whisper not installed:', e, file=sys.stderr)
        print('Install with: pip install faster-whisper', file=sys.stderr)
        sys.exit(1)

    audio = Path(args.audio).expanduser().resolve()
    if not audio.exists():
        print(f'Audio not found: {audio}', file=sys.stderr); sys.exit(2)
    model_path = ensure_model(args.model, Path(args.cache).expanduser().resolve())
    if not model_path:
        print('Model path not resolved. Provide a valid directory or install huggingface_hub to use aliases.', file=sys.stderr)
        sys.exit(3)
    print(f'Using model: {model_path}')
    model = WhisperModel(model_path, device='cpu', compute_type='int8')

    def run(vad, lang):
        print(f'-- transcribe: vad={vad} lang={lang or "auto"}')
        segs, info = model.transcribe(str(audio), language=lang or None, vad_filter=vad)
        out = []
        for s in segs:
            if s.text:
                print(f'[{s.start:.2f}-{s.end:.2f}] {s.text}')
                out.append(s.text.strip())
        txt = ' '.join(out).strip()
        print(f'len={len(txt)}\n')
        return txt

    # try requested settings
    text = run(vad=(not args.no_vad), lang=(args.lang or None))
    if not text and not args.no_vad:
        text = run(vad=False, lang=args.lang)
    if not text and args.lang:
        text = run(vad=False, lang=None)
    print('---- TRANSCRIPT ----')
    print(text)
    print('--------------------')

if __name__ == '__main__':
    main()

