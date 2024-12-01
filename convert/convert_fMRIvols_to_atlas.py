import os
import glob
import nibabel as nib
from tqdm import tqdm
import numpy as np
import argparse


def convert_fMRIvols_to_atlas(fmri_pattern, output_path, atlas_file):
    # Where is the data located?
    fmri_files = glob.glob(fmri_pattern)
    fmri_count = len(fmri_files)
    print("fMRI data pattern specified:", fmri_pattern)
    print("Number of fMRI files:", fmri_count)
    print("Atlas file:", atlas_file)
    try:
        label_img = nib.load(atlas_file)
        label = label_img.get_fdata()
        label = label.flatten()
        unique_labels = np.unique(label)
        nParcels = len(unique_labels) - 1
        assert np.array_equal(
            unique_labels.astype(np.int64), np.arange(nParcels + 1)
        ), f"Labels must be integers from 0 to {nParcels}"
        print("Atlas successfully loaded")
        parcel_to_indices = {i: np.where(label == i + 1)[0] for i in range(0, nParcels)}
    except Exception as e:
        print(f"Loading dlabel Atlas File Error for {atlas_file}: {str(e)}")

    # Create output directory
    os.makedirs(output_path, exist_ok=True)
    # Create fMRI .dat files
    for index, fmri_file in enumerate(fmri_files):
        # Load images and labels
        if ".nii.gz" in fmri_file:
            file_name = os.path.basename(fmri_file)
            print(f"Processing {index + 1}/{fmri_count} {file_name}")
            print(f"Loading 4D image from {fmri_file}")

            try:
                dts_img = nib.load(fmri_file)
                dts = dts_img.get_fdata()
                print("Loaded fMRI data")
            except Exception as e:
                print(f"Loading 4D File Error for {file_name}: {str(e)}")

            try:
                print(f"Extracting Parcels for {file_name}")
                m = dts.reshape((-1, dts.shape[-1])).T
                sh = m.shape[0]

                # Get parcellated time series given a label input
                # print(f'\nGet parcellated time series using {l}\n\n')

                pMeas = np.zeros((nParcels, 3))
                pmTS = np.zeros((sh, nParcels))

                items = parcel_to_indices.items()
                for i, indices in tqdm(items, desc="Extracting parcels", unit="parcel"):
                    pmTS[:, i] = np.nanmean(m[:, indices], axis=1)

                # Replace NaNs with 0
                pmTS[np.isnan(pmTS)] = 0
                # extend the last column to extra 20 columns
                last_column = pmTS[-1, :].reshape(1, -1)
                extra_columns = np.repeat(last_column, 20, axis=0)
                extended_pmTS = np.vstack((pmTS, extra_columns))
                # Save Time Series
                save_name = file_name.split(".nii.gz")[0]
                fn = os.path.join(output_path, f"{save_name}.dat")
                print(
                    f"Saving file {fn} with shape {extended_pmTS.shape} (TRs, parcels)"
                )
                np.savetxt(fn, extended_pmTS)

            except:
                print(f"Error with parcel Extraction for {file_name}")

        else:
            print(f"File {file_name} not a nifti file. Skipping...")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fmri-pattern",
        type=str,
        required=True,
        help="Pattern to search for fMRI files in the data directory",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Path to save the output parcellated time series",
    )
    parser.add_argument(
        "--atlas-file", type=str, required=True, help="Path to the atlas file"
    )
    args = parser.parse_args()
    convert_fMRIvols_to_atlas(args.fmri_pattern, args.output_path, args.atlas_file)


if __name__ == "__main__":
    main()
