#!/usr/bin/env python3
import argparse, subprocess, sys, os, tempfile, wave, shutil
from pathlib import Path

def ffmpeg_available():
    return shutil.which('ffmpeg') is not None

def ffprobe_duration_seconds(src: Path) -> float:
    if shutil.which('ffprobe') is None:
        return 0.0
    try:
        out = subprocess.check_output([
            'ffprobe','-v','error','-show_entries','format=duration','-of','default=nw=1:nk=1', str(src)
        ], stderr=subprocess.DEVNULL)
        return float(out.strip())
    except Exception:
        return 0.0

def convert_to_wav_pcm16(src: Path, dst: Path, rate=16000):
    subprocess.run(['ffmpeg','-y','-i',str(src),'-ac','1','-ar',str(rate),'-f','wav',str(dst)],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

def rms(samples):
    import math
    if not samples:
        return 0.0
    s = 0.0
    for x in samples:
        s += x*x
    return math.sqrt(s/len(samples))

def main():
    ap = argparse.ArgumentParser(description='FFmpeg conversion sanity check (to 16k mono WAV)')
    ap.add_argument('audio', help='Path to input audio/file')
    args = ap.parse_args()
    src = Path(args.audio).expanduser().resolve()
    if not src.exists():
        print(f"Input not found: {src}", file=sys.stderr); sys.exit(2)

    print(f"ffmpeg: {'yes' if ffmpeg_available() else 'no'}")
    d = ffprobe_duration_seconds(src)
    print(f"input: {src} duration: {d:.3f}s")

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / 'test16k.wav'
        convert_to_wav_pcm16(src, tmp, 16000)
        print(f"converted: {tmp}")
        with wave.open(str(tmp), 'rb') as wf:
            ch, sampwidth, rate = wf.getnchannels(), wf.getsampwidth(), wf.getframerate()
            frames = wf.getnframes()
            dur = frames / float(rate)
            print(f"wav params: channels={ch} width={sampwidth} rate={rate} frames={frames} duration={dur:.3f}s")
            # read first 3 seconds and compute RMS
            n = min(int(3*rate), frames)
            raw = wf.readframes(n)
            import array
            a = array.array('h'); a.frombytes(raw)  # 16-bit
            val = rms([x/32768.0 for x in a])
            print(f"RMS (first 3s): {val:.6f} {'(very low, maybe silence)' if val < 0.005 else ''}")

if __name__ == '__main__':
    main()

