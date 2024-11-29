import os
import glob
import nibabel as nib
import numpy as np
import argparse


def convert_fMRIvols_to_atlas(fmri_pattern, output_path, atlas_file):
    # Where is the data located?
    fmri_files = glob.glob(fmri_pattern)
    print("fMRI data pattern specified:", fmri_pattern)
    print("Number of fMRI files:", len(fmri_files))
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
    except Exception as e:
        print(f"Loading dlabel Atlas File Error for {atlas_file}: {str(e)}")

    # Create output directory
    os.makedirs(output_path, exist_ok=True)
    # Create fMRI .npy files
    for fmri_file in fmri_files:
        # Load images and labels
        if ".nii.gz" in fmri_file:
            print(f"Loading 4D image from {fmri_file}")

            try:
                dts_img = nib.load(fmri_file)
                dts = dts_img.get_fdata()
                print("Loaded fMRI data")
            except Exception as e:
                print(f"Loading 4D File Error for {fmri_file}: {str(e)}")

            try:
                print(f"Extracting Parcels for {fmri_file}")
                m = dts.reshape((-1, dts.shape[-1])).T
                sh = m.shape[0]

                # Get parcellated time series given a label input
                # print(f'\nGet parcellated time series using {l}\n\n')

                pMeas = np.zeros((nParcels, 3))
                pmTS = np.zeros((sh, nParcels))

                for i in range(1, nParcels + 1):
                    ind = label == i
                    y = m[:, ind]
                    pmTS[:, i - 1] = np.nanmean(y, axis=1)

                # Replace NaNs with 0
                pmTS[np.isnan(pmTS)] = 0
                # extend the last column to extra 20 columns
                last_column = pmTS[-1, :].reshape(1, -1)
                extra_columns = np.repeat(last_column, 20, axis=0)
                extended_pmTS = np.vstack((pmTS, extra_columns))
                # Save Time Series
                save_name = os.path.basename(fmri_file).split(".nii.gz")[0]
                fn = os.path.join(output_path, f"{save_name}.npy")
                print(
                    f"Saving file {fn} with shape {extended_pmTS.shape} (TRs, parcels)"
                )
                np.save(fn, extended_pmTS)

            except:
                print(f"Error with parcel Extraction for {fmri_file}")

        else:
            print(f"File {fmri_file} not a nifti file. Skipping...")


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
