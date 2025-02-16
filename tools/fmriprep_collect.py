import argparse
import os
import json
import numpy as np
import pandas as pd
from bids import BIDSLayout
from convert import convert_fMRIvols_to_atlas


def serialize(entities: dict):
    return f"sub-{entities['sub']}_ses-{entities['ses']}_run-{entities['run']}"


def deserialize(label: str):
    return dict([it.split("-") for it in label.split("_")])


def sub_entities(entities: dict):
    # subject => sub | session => ses | run => run
    return {k[:3]: entities[k] for k in ["subject", "session", "run"]}


def get_parser():
    parser = argparse.ArgumentParser(
        description="Process fMRIprep outputs for ROI extraction and quality filtering"
    )
    parser.add_argument(
        "--fmriprep-derivative-path",
        type=str,
        required=True,
        help="The directory where the fMRIPrep derivatives is located.",
    )
    parser.add_argument(
        "--bids-atlas-path",
        type=str,
        required=True,
        help="The directory where the BIDS Atlases Dataset is located.",
    )
    parser.add_argument(
        "--atlas-name",
        type=str,
        required=True,
        help="Name of atlas in BIDS Atlas to use",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Path to save collected BOLD dataset",
    )
    parser.add_argument(
        "--fd-threshold",
        type=float,
        required=True,
        help="Framewise displacement threshold",
    )
    parser.add_argument(
        "--dvar-threshold", type=float, required=True, help="DVARS threshold"
    )
    parser.add_argument(
        "--no-cache", action="store_true", help="Disable cache and regenerate all data"
    )

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    os.makedirs(args.output_path, exist_ok=True)

    # 1. Setup BIDS layouts
    fmriprep_layout = BIDSLayout(args.fmriprep_derivative_path)
    fmriprep_layout.add_derivatives(args.fmriprep_derivative_path)
    atlas_layout = BIDSLayout(
        args.bids_atlas_path,
        config=[json.load(open("data/atlas_bids_config.json"))],
        validate=False,
    )

    # 2. Get preprocessed fMRI files
    bold_files = fmriprep_layout.get(
        suffix="bold",
        space="MNI152NLin6Asym",
        desc="preproc",
        extension=".nii.gz",
        res="2",
    )
    if not bold_files:
        raise ValueError("No valid BOLD files found in fMRIprep directory")

    # 3. Get atlas file
    atlas_files = atlas_layout.get(
        atlas=args.atlas_name,
        suffix="dseg",
        extension=".nii.gz",
        space="MNI152NLin6Asym",
        res="2",
    )
    if not atlas_files:
        raise ValueError(f"No atlas file found for {args.atlas_name}")
    atlas_file = atlas_files[0].path

    # 4. Cache handling
    cache_prefix = f"atlas-{args.atlas_name}_space-MNI152NLin6Asym_res-2_full"
    bold_cache_path = os.path.join(args.output_path, f"{cache_prefix}_bold.npz")

    if not args.no_cache and os.path.isfile(bold_cache_path):
        print("Loading cached data...")
        all_data_dict = np.load(bold_cache_path)
        all_data = list(all_data_dict.values())
        all_entities = list(map(deserialize, all_data_dict.keys()))
    else:
        print("Processing all fMRI files...")
        fmri_paths = [f.path for f in bold_files]
        all_data = convert_fMRIvols_to_atlas(fmri_paths, atlas_file)
        all_entities = [sub_entities(f.entities) for f in bold_files]
        all_data_dict = dict(zip(map(serialize, all_entities), all_data))
        np.savez_compressed(bold_cache_path, **all_data_dict)

    # 5. Quality filtering
    valid_indices = []
    for idx, entities in enumerate(all_entities):
        confounds = fmriprep_layout.get(
            subject=entities["sub"],
            session=entities["ses"],
            run=entities["run"],
            task="rest",
            suffix="timeseries",
            desc="confounds",
            extension=".tsv",
        )
        if not confounds:
            print(f"Skipping {entities}: No confounds file found")
            continue

        try:
            df = pd.read_csv(confounds[0].path, sep="\t")
            fd_filter = df["framewise_displacement"] > args.fd_threshold
            dvar_filter = df["dvars"] > args.dvar_threshold
            if (fd_filter | dvar_filter).mean() <= 0.5:
                valid_indices.append(idx)
        except Exception as e:
            print(f"Error processing {entities}: {str(e)}")

    # 6. Save filtered results
    filtered_data = [all_data[i] for i in valid_indices]
    filtered_entities = [all_entities[i] for i in valid_indices]
    filtered_data_dict = dict(zip(map(serialize, filtered_entities), filtered_data))

    output_prefix = (
        f"atlas-{args.atlas_name}_space-MNI152NLin6Asym_res-2_"
        f"fd-{args.fd_threshold}_dvar-{args.dvar_threshold}_filtered"
    )
    bold_output_path = os.path.join(args.output_path, f"{output_prefix}_bold.npz")
    np.savez_compressed(bold_output_path, **filtered_data_dict)


if __name__ == "__main__":
    main()
