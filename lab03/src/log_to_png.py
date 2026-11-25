"""Render a real run log to a PNG so it can be used as terminal evidence.

The image is the log text unchanged, with a footer naming the source file,
its SHA-256 and the render time, so the image can be checked against the log.
Run: python3 src/log_to_png.py runs/ex01_janfeb.log figures/lab02_ex01_janfeb.png
Needs Pillow:  ~/bda/venv/bin/pip install pillow
"""
import datetime
import hashlib
import re
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
raw = src.read_bytes()
lines = raw.decode("utf-8", errors="replace").expandtabs(4).splitlines()
stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
footer = [f"source: {src}  sha256: {hashlib.sha256(raw).hexdigest()}",
          f"rendered from the log file at {stamp}; text not edited"]

# Terminal colour codes (from bda-status.sh) are drawn as colours, not as text.
SGR = re.compile(r"\x1b\[([0-9;]*)m")
COLOURS = {"31": (240, 90, 90), "32": (110, 210, 110), "33": (230, 200, 90)}
DEFAULT = (230, 230, 230)


def segments(line):
    colour, pos = DEFAULT, 0
    for m in SGR.finditer(line):
        if m.start() > pos:
            yield line[pos:m.start()], colour
        codes = m.group(1).split(";")
        colour = next((COLOURS[c] for c in codes if c in COLOURS), DEFAULT)
        pos = m.end()
    if pos < len(line):
        yield line[pos:], colour


font = None
for f in ["/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
          "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf"]:
    if Path(f).exists():
        font = ImageFont.truetype(f, 15)
        break
font = font or ImageFont.load_default()

probe = ImageDraw.Draw(Image.new("RGB", (1, 1)))
line_h = probe.textbbox((0, 0), "Ag", font=font)[3] + 5
plain = [SGR.sub("", l) for l in lines]
width = max(probe.textlength(l, font=font) for l in plain + footer + [" " * 40])
pad = 18
img = Image.new("RGB", (int(width) + 2 * pad, line_h * (len(lines) + len(footer) + 1) + 2 * pad),
                (30, 30, 30))
d = ImageDraw.Draw(img)
y = pad
for l in lines:
    x = pad
    for text, colour in segments(l):
        d.text((x, y), text, font=font, fill=colour)
        x += probe.textlength(text, font=font)
    y += line_h
y += line_h // 2
d.line((pad, y - 4, img.width - pad, y - 4), fill=(110, 110, 110))
for l in footer:
    d.text((pad, y), l, font=font, fill=(150, 150, 150))
    y += line_h
dst.parent.mkdir(parents=True, exist_ok=True)
img.save(dst)
print(f"wrote {dst} ({img.width}x{img.height}) from {src}")
