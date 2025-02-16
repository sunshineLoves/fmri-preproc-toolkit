import argparse
import os
import json
import numpy as np
import pandas as pd
from bids import BIDSLayout
from convert import convert_fMRIvols_to_atlas


def load_atlas_config():
    """Load BIDS atlas configuration"""
    return json.load(open("atlas_bids_config.json"))


def main():
    parser = argparse.ArgumentParser(
        description="Process fMRIprep outputs for ROI extraction and quality filtering"
    )
    parser.add_argument("fmriprep_dir", help="Path to fMRIprep derivatives directory")
    parser.add_argument("atlas_dir", help="Path to BIDS-format atlas dataset")
    parser.add_argument("atlas_name", help="Name of atlas to use")
    parser.add_argument(
        "--fd-threshold",
        type=float,
        required=True,
        help="Framewise displacement threshold",
    )
    parser.add_argument(
        "--dvar-threshold", type=float, required=True, help="DVARS threshold"
    )
    parser.add_argument("output_dir", help="Path to save output files")
    parser.add_argument(
        "--use-cache", action="store_true", help="Use cached preprocessed data"
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # 1. Setup BIDS layouts
    fmriprep_layout = BIDSLayout(args.fmriprep_dir)
    fmriprep_layout.add_derivatives(args.fmriprep_dir)
    atlas_layout = BIDSLayout(
        args.atlas_dir, config=[load_atlas_config()], validate=False
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
    bold_cache_path = os.path.join(args.output_dir, f"{cache_prefix}_bold.npy")
    entities_cache_path = os.path.join(args.output_dir, f"{cache_prefix}_entities.json")

    if (
        args.use_cache
        and os.path.exists(bold_cache_path)
        and os.path.exists(entities_cache_path)
    ):
        print("Loading cached data...")
        all_data = np.load(bold_cache_path)
        with open(entities_cache_path, "r") as f:
            all_entities = json.load(f)
    else:
        print("Processing all fMRI files...")
        fmri_paths = [f.path for f in bold_files]
        all_data = convert_fMRIvols_to_atlas(fmri_paths, atlas_file)
        all_entities = [f.entities for f in bold_files]
        np.save(bold_cache_path, all_data)
        with open(entities_cache_path, "w") as f:
            json.dump(all_entities, f)

    # 5. Quality filtering
    valid_indices = []
    for idx, entities in enumerate(all_entities):
        confounds = fmriprep_layout.get(
            subject=entities["subject"],
            session=entities.get("session"),
            task=entities.get("task"),
            run=entities.get("run"),
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
    filtered_data = all_data[valid_indices]
    filtered_entities = [all_entities[i] for i in valid_indices]

    output_prefix = (
        f"atlas-{args.atlas_name}_space-{bold_files[0].entities['space']}_res-2_"
        f"fd-{args.fd_threshold}_dvar-{args.dvar_threshold}_filtered"
    )
    np.save(os.path.join(args.output_dir, f"{output_prefix}_bold.npy"), filtered_data)
    with open(
        os.path.join(args.output_dir, f"{output_prefix}_entities.json"), "w"
    ) as f:
        json.dump(filtered_entities, f)


if __name__ == "__main__":
    main()
