"""One-off script to crop README screenshots from Cursor workspace images."""
from __future__ import annotations

import glob
import os
from pathlib import Path

from PIL import Image

SRC = Path(
    r"C:\Users\Nikola Milosavljevic\AppData\Roaming\Cursor\User"
    r"\workspaceStorage\empty-window\images"
)
OUT = Path(__file__).resolve().parent.parent / "docs" / "screenshots"

MAPPING: list[tuple[str, str, str]] = [
    ("IMG_7487*.png", "01-list-destination-recommended.png", "list"),
    ("IMG_7486*.png", "02-search-results.png", "list"),
    ("IMG_7485*.png", "03-list-trg-republike.png", "list"),
    ("IMG_7479*.png", "04-detail-bezanijska.png", "detail"),
    ("IMG_7484*.png", "05-detail-obilicev-venac.png", "detail"),
]

CROPS = {
    "list": {"top": 130, "bottom": 40},
    "detail": {"top": 68, "bottom": 40},
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for pattern, out_name, kind in MAPPING:
        matches = glob.glob(str(SRC / pattern))
        if not matches:
            raise FileNotFoundError(f"No match for {pattern} in {SRC}")
        path = matches[0]
        im = Image.open(path)
        w, h = im.size
        c = CROPS[kind]
        cropped = im.crop((0, c["top"], w, h - c["bottom"]))
        out_path = OUT / out_name
        cropped.save(out_path, optimize=True)
        print(f"{out_name}: {im.size} -> {cropped.size}")


if __name__ == "__main__":
    main()
