import pandas as pd
from pydriller import Repository
from tqdm import tqdm
from matplotlib import pyplot as plt

REPOS = ["../llama-cpp-python", "../aeon", "../faststream"]

def run_pydriller(repo_path):
    repo_name = repo_path.split("/")[-1]
    print(f"Analyzing repository: {repo_name}")
    
    df = pd.DataFrame(columns=["old_file path", "new_file path", "commit SHA", "parent commit SHA", "commit message", "diff_myers", "diff_hist", "Discrepancy1", "Discrepancy2"])

    repo_myers_diff = Repository(repo_path, skip_whitespaces=True)
    repo_hist_diff = Repository(repo_path, histogram_diff=True, skip_whitespaces=True)

    for commit_myers, commit_hist in tqdm(zip(repo_myers_diff.traverse_commits(), repo_hist_diff.traverse_commits()), desc=f"Processing {repo_name}"):
        assert commit_myers.hash == commit_hist.hash
        assert commit_myers.msg == commit_hist.msg

        for file_myers, file_hist in zip(commit_myers.modified_files, commit_hist.modified_files):
            assert file_myers.old_path == file_hist.old_path
            assert file_myers.new_path == file_hist.new_path
            assert file_myers.change_type == file_hist.change_type

            if file_myers.change_type.name != 'MODIFY':
                continue

            discrepancy1 = file_myers.diff != file_hist.diff
            discrepancy2 = ([line for _, line in file_myers.diff_parsed['added']] != [line for _, line in file_hist.diff_parsed['added']]) or \
                            ([line for _, line in file_myers.diff_parsed['deleted']] != [line for _, line in file_hist.diff_parsed['deleted']])

            df.loc[len(df)] = {
                "old_file path": file_myers.old_path,
                "new_file path": file_myers.new_path,
                "commit SHA": commit_myers.hash,
                "parent commit SHA": ';'.join(commit_myers.parents) if commit_myers.parents else None,
                "commit message": commit_myers.msg,
                "diff_myers": file_myers.diff,
                "diff_hist": file_hist.diff,
                "Discrepancy1": discrepancy1,
                "Discrepancy2": discrepancy2
            }

    return df