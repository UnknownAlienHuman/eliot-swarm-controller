"""Generate the temporary k144 icon with Pillow 13 (python -I assets/make_icon.py)."""

from pathlib import Path

from PIL import Image, ImageDraw

SIZES = (16, 20, 24, 32, 40, 48, 64, 256)
PRIMARY = "#6750A4"  # Material 3 baseline light primary

image = Image.new("RGBA", (1024, 1024))
ImageDraw.Draw(image).rounded_rectangle((64, 64, 959, 959), radius=224, fill=PRIMARY)
image = image.resize((256, 256), Image.Resampling.LANCZOS)
image.save(Path(__file__).with_name("eliot-swarm.ico"), sizes=[(n, n) for n in SIZES])
