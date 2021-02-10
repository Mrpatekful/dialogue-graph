import os
import shutil
import tempfile
import argparse
import requests
import json
import tqdm
import csv
import pathlib
import pprint

URL = "https://raw.githubusercontent.com/facebookresearch/opendialkg/master/data/"

DATA = "opendialkg.csv"
ENTITIES = "opendialkg_entities.txt"
RELATIONS = "opendialkg_relations.txt"
TRIPLES = "opendialkg_triples.txt"
DIALOGUES = "dialogues.json"

FILES = [DATA, ENTITIES, RELATIONS, TRIPLES]


def download(file_name, url):
    with requests.Session() as session:
        response = session.get(url, stream=True, timeout=5)
        loop = tqdm.tqdm(desc="Downloading", unit="B", unit_scale=True)

        with open(file_name, "wb") as fh:
            for chunk in response.iter_content(1024):
                if chunk:
                    loop.update(len(chunk))
                    fh.write(chunk)


def build_dialogues(input_path, output_path):
    dialogues = []

    with open(input_path, newline="") as fh:
        reader = csv.DictReader(fh)
        key = "message"

        for row in reader:
            pprint.pprint(json.loads(row["Messages"]))
            dialogue = [m[key] for m in json.loads(row["Messages"]) if key in m]
            if len(dialogue) > 1:
                dialogues.append(dialogue)

    with open(output_path, "w") as fh:
        json.dump({"dialogue": dialogues}, fh, indent=4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_dir",
        required=True,
        type=pathlib.Path,
        help="Directory name of the output .txt files.",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    if not all((args.output_dir / file_name).exists() for file_name in FILES):
        for file_name in FILES:
            download(args.output_dir / file_name, f"{URL}{file_name}")

    dialogues_path = args.output_dir / DIALOGUES

    # if not dialogues_path.exists():
    build_dialogues(args.output_dir / DATA, dialogues_path)


if __name__ == "__main__":
    main()
