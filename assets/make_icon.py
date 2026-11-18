"""Draw the Eliot Swarm Controller icon with Pillow 13.

    python -I assets/make_icon.py [preview.png]

The mark is a hub: a white controller node on a Material 3 primary tile, with six
swarm nodes on a hexagon converging on it through spokes. Each size is drawn from
its own geometry (pixel-snapped nodes, no spokes below 32 px) by supersampling and
box-filtering, so the output is identical on every run. The optional argument also
writes a light/dark preview of the frames actually stored in the .ico.
"""

import io
import math
import sys
from pathlib import Path

from PIL import Image, ImageDraw

ICO = Path(__file__).with_name("eliot-swarm.ico")
SIZES = (16, 20, 24, 32, 40, 48, 64, 256)
SUPERSAMPLE = 16

# Material 3 baseline primary tonal palette.
TILE = "#6750A4"  # primary 40
SPOKE = "#9A82DB"  # primary 60
SWARM = "#D0BCFF"  # primary 80
HUB = "#FFFFFF"  # primary 100

# Master geometry on a 256 px grid: tile inset and corner radius, hub radius,
# swarm-node radius, hexagon radius and spoke width.
MASTER = dict(inset=8, corner=56, hub=36, node=20, orbit=80, spoke=12)


def scaled(n):
    """Proportional geometry for n px, with nodes kept large enough to read."""
    s = n / 256
    g = {key: value * s for key, value in MASTER.items()}
    g["inset"] = round(g["inset"])
    g["node"] = max(g["node"], 2.5)
    g["spoke"] = max(g["spoke"], 2)
    top = g["orbit"]
    side = (top * math.cos(math.pi / 6), top * math.sin(math.pi / 6))
    g["points"] = hexagon(top, side)
    return g


def hexagon(top, side):
    sx, sy = side
    return [(0, -top), (sx, -sy), (sx, sy), (0, top), (-sx, sy), (-sx, -sy)]


# Hinted small sizes: even node diameters on whole-pixel centres, a 3-4-5 hexagon
# at 16 px, and no spokes (they would only blur the gaps between nodes).
HINTED = {
    16: dict(inset=0, corner=3, hub=2, node=1, points=hexagon(5, (4, 3)), spoke=0),
    20: dict(inset=0, corner=4, hub=3, node=1, points=hexagon(6, (5, 3)), spoke=0),
    24: dict(inset=0, corner=5, hub=4, node=2, points=hexagon(8, (7, 4)), spoke=0),
}


def draw(n):
    g = HINTED.get(n) or scaled(n)
    f = SUPERSAMPLE
    big = Image.new("RGBA", (n * f, n * f), (0, 0, 0, 0))
    d = ImageDraw.Draw(big)

    def box(x0, y0, x1, y1):
        return (round(x0 * f), round(y0 * f), round(x1 * f) - 1, round(y1 * f) - 1)

    i = g["inset"]
    d.rounded_rectangle(box(i, i, n - i, n - i), radius=round(g["corner"] * f), fill=TILE)
    c = n / 2
    half = g["spoke"] / 2
    for dx, dy in g["points"] if half else ():
        length = math.hypot(dx, dy)
        nx, ny = -dy / length * half, dx / length * half
        corners = [(c + nx, c + ny), (c + dx + nx, c + dy + ny), (c + dx - nx, c + dy - ny), (c - nx, c - ny)]
        d.polygon([(round(x * f), round(y * f)) for x, y in corners], fill=SPOKE)
    r = g["node"]
    for dx, dy in g["points"]:
        d.ellipse(box(c + dx - r, c + dy - r, c + dx + r, c + dy + r), fill=SWARM)
    r = g["hub"]
    d.ellipse(box(c - r, c - r, c + r, c + r), fill=HUB)
    return big.resize((n, n), Image.Resampling.BOX)


def preview(path):
    """Frames read back from the .ico at 16/32/64/256 px on light and dark, plus 8x zooms."""
    data = ICO.read_bytes()
    frames = {}
    for n in (16, 32, 64, 256):
        icon = Image.open(io.BytesIO(data))
        icon.size = (n, n)
        frames[n] = icon.convert("RGBA")
    zooms = [frames[16].resize((128, 128), Image.Resampling.NEAREST), frames[32].resize((256, 256), Image.Resampling.NEAREST)]
    row = [frames[n] for n in (16, 32, 64, 256)] + zooms
    pad = 24
    width = pad + sum(im.width + pad for im in row)
    height = 256 + 2 * pad
    sheet = Image.new("RGB", (width, 2 * height))
    for y0, background in ((0, "#F3F3F3"), (height, "#202020")):
        sheet.paste(background, (0, y0, width, y0 + height))
        x = pad
        for im in row:
            sheet.paste(im, (x, y0 + pad + (256 - im.height) // 2), im)
            x += im.width + pad
    # Write only into an existing folder: the script never creates directories.
    if not Path(path).parent.is_dir():
        sys.exit(f"preview folder does not exist: {Path(path).parent}")
    sheet.save(path)


def main():
    frames = [draw(n) for n in SIZES]
    frames[-1].save(ICO, sizes=[(n, n) for n in SIZES], append_images=frames[:-1])
    if len(sys.argv) > 1:
        preview(sys.argv[1])


if __name__ == "__main__":
    main()
