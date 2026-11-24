"""Builds the desktop's shipped fonts from Google's originals (fontTools 4.60.1).

Inputs (one directory, passed as the first argument), downloaded 2026-11-24 at these revisions:
  Roboto.ttf          github.com/google/fonts 6183fc0d2636 ofl/roboto/Roboto[wdth,wght].ttf
                      (SIL OFL 1.1)
  RobotoMono.ttf      github.com/google/fonts b01086459d9d ofl/robotomono/RobotoMono[wght].ttf
                      (SIL OFL 1.1)
  MaterialSymbolsRounded.ttf and .codepoints
                      github.com/google/material-design-icons 10ecc1479061 variablefont/
                      MaterialSymbolsRounded[FILL,GRAD,opsz,wght] (Apache License 2.0)
Output: this directory.

- Roboto keeps its weight axis (400-700) at normal width; Roboto Mono keeps 400-700.
  Both are subset to Latin, Latin Extended, Greek, Cyrillic and common symbols.
- The icons are Material Symbols Rounded at optical size 20 and weight 400, cut to the
  glyphs of the brand book's `glyph` tokens (../tokens.json): each outline glyph (FILL 0) at its own code point and the filled glyph
  (FILL 1) at 0xF0000 + (code point - 0xE000). "Eliot Symbols" is grade 0 (light themes),
  "Eliot Symbols Dark" grade -25 (dark themes), as the brand book specifies.
"""

import io
import json
import sys
from pathlib import Path

from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont
from fontTools import subset

FILLED_BASE = 0xF0000
TEXT_UNICODES = (
    list(range(0x20, 0x7F)) + list(range(0xA0, 0x250)) + list(range(0x370, 0x400))
    + list(range(0x400, 0x530)) + list(range(0x1E00, 0x1F00)) + list(range(0x2000, 0x2070))
    + [0x20AC, 0x20BD, 0x2116, 0x2122, 0x2190, 0x2191, 0x2192, 0x2193, 0x2212, 0x2248,
       0x2260, 0x2264, 0x2265, 0x2318, 0x2325, 0x2326, 0x232B, 0x23CE, 0x2713, 0x2715, 0x25BE]
)


def describe(font: TTFont, description: str, licence: str, url: str) -> None:
    """Record that the file is a modified subset, and under which licence."""
    names = font["name"]
    for name_id in (10, 13, 14):
        names.removeNames(nameID=name_id)
    names.setName(description, 10, 3, 1, 0x409)
    names.setName(licence, 13, 3, 1, 0x409)
    names.setName(url, 14, 3, 1, 0x409)


OFL = ("This Font Software is licensed under the SIL Open Font License, Version 1.1.",
       "https://openfontlicense.org")
APACHE = ("Licensed under the Apache License, Version 2.0.",
          "https://www.apache.org/licenses/LICENSE-2.0")


def rename(font: TTFont, family: str) -> None:
    names = font["name"]
    for record in list(names.names):
        if record.nameID in (1, 3, 4, 6, 16, 17):
            names.removeNames(nameID=record.nameID)
    names.setName(family, 1, 3, 1, 0x409)
    names.setName("Regular", 2, 3, 1, 0x409)
    names.setName(f"{family} Regular", 3, 3, 1, 0x409)
    names.setName(family, 4, 3, 1, 0x409)
    names.setName(family.replace(" ", ""), 6, 3, 1, 0x409)


def subset_font(font: TTFont, unicodes, keep_layout: bool) -> None:
    options = subset.Options()
    options.layout_features = ["*"] if keep_layout else []
    options.name_IDs = ["*"]
    options.notdef_outline = True
    options.glyph_names = False
    subsetter = subset.Subsetter(options)
    subsetter.populate(unicodes=unicodes)
    subsetter.subset(font)


def instance(src: Path, limits: dict) -> TTFont:
    """The instanced font, saved and read back so the subsetter sees plain tables."""
    font = TTFont(src, recalcTimestamp=False)
    instantiateVariableFont(font, limits, inplace=True)
    buffer = io.BytesIO()
    font.save(buffer)
    buffer.seek(0)
    return TTFont(buffer, recalcTimestamp=False)


def text_font(src: Path, out: Path, limits: dict, original: str) -> None:
    font = instance(src, limits)
    subset_font(font, TEXT_UNICODES, keep_layout=True)
    describe(font, f"Modified by Eliot Swarm: a subset of {original}, instanced at normal width.", *OFL)
    font.save(out)


def icon_instance(src: Path, fill: float, grade: float, codes) -> TTFont:
    font = instance(src, {"FILL": fill, "GRAD": grade, "opsz": 20, "wght": 400})
    subset_font(font, codes, keep_layout=False)
    return font


def icon_font(src: Path, codes, grade: float, family: str, out: Path) -> None:
    outline = icon_instance(src, 0, grade, codes)
    filled = icon_instance(src, 1, grade, codes)
    glyf_o, glyf_f = outline["glyf"], filled["glyf"]
    hmtx_o, hmtx_f = outline["hmtx"], filled["hmtx"]
    cmap_f = filled.getBestCmap()
    renamed = {name: f"{name}.fill" for name in filled.getGlyphOrder() if name != ".notdef"}
    for old, new in renamed.items():
        glyph = glyf_f[old]
        if glyph.isComposite():
            for component in glyph.components:
                component.glyphName = renamed.get(component.glyphName, component.glyphName)
        glyf_o[new] = glyph  # also appends `new` to the glyf table's glyph order
        hmtx_o.metrics[new] = hmtx_f.metrics[old]
    order = list(glyf_o.glyphOrder)
    outline.setGlyphOrder(order)
    # The outline glyphs keep their code points (format 4); the filled ones go to the
    # supplementary private use area, which only a format 12 subtable can hold.
    from fontTools.ttLib.tables._c_m_a_p import CmapSubtable

    full = dict(outline.getBestCmap())
    for code, name in cmap_f.items():
        assert code <= 0xFFFF, f"{name} is outside the BMP"
        full[FILLED_BASE + code - 0xE000] = renamed[name]
    twelve = CmapSubtable.newSubtable(12)
    twelve.platformID, twelve.platEncID, twelve.language = 3, 10, 0
    twelve.cmap = full
    four = [t for t in outline["cmap"].tables if t.format == 4 and t.isUnicode()]
    for table in four:
        table.cmap = {c: n for c, n in table.cmap.items() if c <= 0xFFFF}
    outline["cmap"].tables = four + [twelve]
    outline["maxp"].numGlyphs = len(order)
    rename(outline, family)
    describe(
        outline,
        "Modified by Eliot Swarm: a subset of Material Symbols Rounded, instanced at optical "
        f"size 20, weight 400 and grade {grade:g}; filled glyphs added at 0xF0000 + code - 0xE000.",
        *APACHE,
    )
    outline.save(out)


def main() -> None:
    src = Path(sys.argv[1])
    here = Path(__file__).parent
    points = dict(line.split() for line in (src / "MaterialSymbolsRounded.codepoints").read_text().splitlines() if line.strip())
    # The brand book's icons: each token names its Material glyph and code point.
    glyphs = json.loads((here.parent / "tokens.json").read_text(encoding="utf-8"))["glyph"]["tokens"]
    for glyph in glyphs:
        assert points.get(glyph["material"]) == glyph["value"].lower(), glyph
    codes = sorted({int(glyph["value"], 16) for glyph in glyphs})
    text_font(src / "Roboto.ttf", here / "Roboto.ttf", {"wght": (400, 700), "wdth": 100}, "Roboto")
    text_font(src / "RobotoMono.ttf", here / "RobotoMono.ttf", {"wght": (400, 700)}, "Roboto Mono")
    icon_font(src / "MaterialSymbolsRounded.ttf", codes, 0, "Eliot Symbols", here / "EliotSymbols.ttf")
    icon_font(src / "MaterialSymbolsRounded.ttf", codes, -25, "Eliot Symbols Dark", here / "EliotSymbolsDark.ttf")


if __name__ == "__main__":
    main()
