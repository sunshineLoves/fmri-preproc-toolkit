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


def quality_filter(xcp_d_layout, all_data, all_entities):
    valid_indices = []
    for idx, entities in enumerate(all_entities):
        confounds = xcp_d_layout.get(
            subject=entities["sub"],
            session=entities["ses"],
            run=entities["run"],
            task="rest",
            suffix="outliter",
            extension=".tsv",
        )
        if not confounds:
            print(f"Skipping {entities}: No confounds file found")
            continue

        try:
            df = pd.read_csv(confounds[0].path, sep="\t")
            if df.framewise_displacement.mean() <= 0.5:
                valid_indices.append(idx)
        except Exception as e:
            print(f"Error processing {entities}: {str(e)}")

    # 6. Save filtered results
    filtered_data = [all_data[i] for i in valid_indices]
    filtered_entities = [all_entities[i] for i in valid_indices]
    return dict(zip(map(serialize, filtered_entities), filtered_data))


def get_parser():
    parser = argparse.ArgumentParser(
        description="Process XCP-D outputs for ROI extraction and quality filtering"
    )
    parser.add_argument(
        "--xcp_d-derivative-path",
        type=str,
        required=True,
        help="The directory where the XCP-D derivatives is located.",
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
        "--no-cache", action="store_true", help="Disable cache and regenerate all data"
    )

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    os.makedirs(args.output_path, exist_ok=True)

    # 1. Setup BIDS layouts
    xcp_d_layout = BIDSLayout(args.xcp_d_derivative_path)
    xcp_d_layout.add_derivatives(
        args.xcp_d_derivative_path,
        config=["bids", "derivatives", json.load(open("data/xcp_d_bids_config2.json"))],
    )
    atlas_layout = BIDSLayout(
        args.bids_atlas_path,
        config=[json.load(open("data/atlas_bids_config.json"))],
        validate=False,
    )

    # 2. Get preprocessed fMRI files and timeseries tsv
    bold_files = xcp_d_layout.get(
        space="MNI152NLin6Asym",
        res="2",
        desc="denoisedSmoothed",
        suffix="bold",
        extension=".nii.gz",
    )

    time_tables = xcp_d_layout.get(
        space="MNI152NLin6Asym",
        segmentation=args.atlas_name,
        statistic="mean",
        suffix="timeseries",
        extension=".tsv",
    )

    if not bold_files:
        raise ValueError("No valid BOLD files found in XCP-D directory")
    if not time_tables:
        raise ValueError("No valid timeseries files found in XCP-D directory")

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
    cache_prefix = (
        f"preproc-xcpd_atlas-{args.atlas_name}_space-MNI152NLin6Asym_res-2_full"
    )
    bold_cache_path = os.path.join(args.output_path, f"{cache_prefix}_bold.npz")
    time_cache_path = os.path.join(args.output_path, f"{cache_prefix}_time.npz")

    if (
        not args.no_cache
        and os.path.isfile(bold_cache_path)
        and os.path.isfile(time_cache_path)
    ):
        print("Loading cached data...")
        all_bold_data_dict = np.load(bold_cache_path)
        all_bold_data = list(all_bold_data_dict.values())
        all_bold_entities = list(map(deserialize, all_bold_data.keys()))
        all_time_data_dict = np.load(time_cache_path)
        all_time_data = list(all_time_data_dict.values())
        all_time_entities = list(map(deserialize, all_time_data.keys()))
    else:
        print("Processing all fMRI files...")
        fmri_paths = [f.path for f in bold_files]
        all_bold_data = convert_fMRIvols_to_atlas(fmri_paths, atlas_file)
        all_bold_entities = [sub_entities(f.entities) for f in bold_files]
        all_bold_data_dict = dict(zip(map(serialize, all_bold_entities), all_bold_data))
        np.savez_compressed(bold_cache_path, **all_bold_data_dict)
        print(f"Loading all timeseries data...")
        tsv_paths = [t.path for t in time_tables]
        all_time_data = [
            np.nan_to_num(pd.read_csv(t, sep="\t").to_numpy(), 0) for t in tsv_paths
        ]
        all_time_entities = [sub_entities(t.entities) for t in time_tables]
        all_time_data_dict = dict(zip(map(serialize, all_time_entities), all_time_data))
        np.savez_compressed(time_cache_path, **all_time_data_dict)

    # 5. Quality filtering
    print("Filtering BOLD data...")
    filtered_bold_data_dict = quality_filter(
        xcp_d_layout, all_bold_data, all_bold_entities
    )
    filtered_bold_count = len(filtered_bold_data_dict.keys())
    print(f"Filtered {filtered_bold_count} out of {len(all_bold_data)} BOLD data")
    print("Filtering TIME data...")
    filtered_time_data_dict = quality_filter(
        xcp_d_layout, all_time_data, all_time_entities
    )
    filtered_time_count = len(filtered_time_data_dict.keys())
    print(f"Filtered {filtered_time_count} out of {len(all_time_data)} TIME data")

    output_prefix = f"preproc-xcpd_atlas-{args.atlas_name}_space-MNI152NLin6Asym_res-2_fd-0.2_filtered"
    bold_output_path = os.path.join(args.output_path, f"{output_prefix}_bold.npz")
    time_output_path = os.path.join(args.output_path, f"{output_prefix}_time.npz")
    np.savez_compressed(bold_output_path, **filtered_bold_data_dict)
    np.savez_compressed(time_output_path, **filtered_time_data_dict)


if __name__ == "__main__":
    main()
